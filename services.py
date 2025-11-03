# activity_monitoring/services.py
"""Business logic and data processing services"""
from typing import Optional, List, Dict, Tuple
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import numpy as np
from scipy import stats

from models import ProductionRecord
from database import get_db_connection, lock
from data_processor import ProductionDataProcessor
from utils import get_production_start_time, get_production_date_range
from config import db_config

logger = logging.getLogger(__name__)

def process_table_data(
    database: str, 
    table: str, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None
) -> List[ProductionRecord]:
    """Process data from a single table with enhanced statistics and 7 AM start time"""
    try:
        with get_db_connection() as cursor:
            columns_info = ProductionDataProcessor.get_table_columns(cursor, database, table)
            
            if not ProductionDataProcessor.validate_required_columns(columns_info):
                return []

            date_column = ProductionDataProcessor.find_date_column(columns_info)
            if not date_column:
                return []

            # Build where clause with 7 AM production day logic
            if start_date and end_date:
                start_dt, end_dt = get_production_date_range(start_date, end_date)
                where_clause = f"`{date_column}` BETWEEN %s AND %s"
                params = (start_dt, end_dt)
            else:
                # Default to current production day (from 7 AM today)
                start_dt, end_dt = get_production_start_time()
                where_clause = f"`{date_column}` BETWEEN %s AND %s"
                params = (start_dt, end_dt)

            # Main grouped query with duration calculation
            query = f"""
                SELECT 
                    operator_en,
                    COUNT(DISTINCT serial_num) as current_output,
                    MIN(`{date_column}`) as start_time,
                    MAX(`{date_column}`) as end_time,
                    TIMESTAMPDIFF(HOUR, MIN(`{date_column}`), NOW()) as duration_hours
                FROM `{database}`.`{table}`
                WHERE {where_clause} AND `status` = 1
                GROUP BY operator_en
                HAVING current_output > 0
            """
            
            cursor.execute(query, params)
            rows = cursor.fetchall()

            results = []
            for operator_en, current_output, start_time, end_time, duration_hours in rows:
                # Parse table name for model and station
                parts = table.split('_', 1)
                model = parts[0]
                station = parts[1] if len(parts) == 2 else ''

                # Get detailed records for this operator
                detail_query = f"""
                    SELECT serial_num, `{date_column}`
                    FROM `{database}`.`{table}`
                    WHERE {where_clause} AND operator_en = %s AND `status` = 1
                    ORDER BY `{date_column}`
                """
                detail_params = params + (operator_en,)
                cursor.execute(detail_query, detail_params)
                
                records = cursor.fetchall()
                serial_nums = [r[0] for r in records]
                timestamps = [r[1] for r in records]

                # Calculate cycle time statistics
                # Calculate cycle time statistics
                cycle_stats = ProductionDataProcessor.calculate_cycle_statistics(timestamps, current_output)
                cycle_time = cycle_stats["avg_cycle_time"]
                individual_durations = cycle_stats.get("durations", [])

                # Check if operator has clicked STOP in projectsdb.break_logs
                try:
                    cursor.execute("""
                        SELECT action_type 
                        FROM projectsdb.break_logs 
                        WHERE operator_en = %s 
                        ORDER BY timestamp DESC 
                        LIMIT 1
                    """, (operator_en,))
                    latest_action = cursor.fetchone()
                    if latest_action and latest_action[0].lower() == "stop":
                        # If STOP is the latest action, freeze the cycle time
                        cycle_time = 0
                except Exception as check_err:
                    logger.warning(f"Could not verify STOP status for {operator_en}: {check_err}")

                # Alternative calculation: total time / output count
                if start_time and end_time and cycle_time > 0:
                    total_duration_calc = (end_time - start_time).total_seconds() / current_output
                    # Use the more conservative (higher) cycle time
                    cycle_time = max(cycle_time, total_duration_calc)


                # Get target time
                target_time = ProductionDataProcessor.get_target_time(cursor, database, model, station)

                # Determine status
                # Determine status based on cycle time vs target
                status = "NO TARGET"
                if target_time is not None:
                    # Orange threshold: target + 20% of target
                    orange_threshold = target_time + (target_time * 0.2)  # e.g., 50 + 10 = 60
                    
                    if cycle_time <= target_time:
                        # Cycle time is at or below target - GREEN
                        status = "ON TARGET"
                    elif cycle_time <= orange_threshold:
                        # Cycle time is between target and target+20% - ORANGE
                        status = "ORANGE TARGET"
                    else:
                        # Cycle time exceeds target+20% - RED
                        status = "BELOW TARGET"
                
                record = ProductionRecord(
                    customer=database,
                    model=model,
                    station=station,
                    operator=operator_en,
                    output=current_output,
                    target_time=target_time,
                    cycle_time=cycle_time,
                    start_time=start_time,
                    end_time=end_time,
                    status=status,
                    serial_nums=serial_nums,
                    duration_hours=duration_hours,
                    individual_durations=individual_durations
                )
                results.append(record)

            return results

    except Exception as e:
        logger.error(f"Error processing table {database}.{table}: {e}")
        return []

@lru_cache(maxsize=128, typed=True)
def get_databases_and_tables() -> Tuple[List[str], Dict[str, List[str]]]:
    """Get all databases and their tables (cached)"""
    try:
        with get_db_connection() as cursor:
            cursor.execute("SHOW DATABASES")
            all_databases = [db[0] for db in cursor.fetchall()]
            databases = [db for db in all_databases if db not in db_config.hidden_databases]
            
            tables_by_db = {}
            for db in databases:
                try:
                    cursor.execute(f"SHOW TABLES FROM `{db}`")
                    tables = [tbl[0] for tbl in cursor.fetchall()]
                    tables_by_db[db] = tables
                except Exception as e:
                    logger.warning(f"Could not access tables in database {db}: {e}")
                    tables_by_db[db] = []
            
            return databases, tables_by_db
    except Exception as e:
        logger.error(f"Error getting databases and tables: {e}")
        return [], {}

def fetch_production_data(
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None
) -> Tuple[List[ProductionRecord], str, List[str], List[str], List[str]]:
    """Fetch all production data with parallel processing and 7 AM start time"""
    databases, tables_by_db = get_databases_and_tables()
    
    all_records = []
    active_databases = set()
    models = set()
    stations = set()
    
    # Get production day info for display
    if start_date and end_date:
        date_display = f"{start_date} → {end_date} (7AM-7AM cycles)"
    else:
        start_dt, end_dt = get_production_start_time()
        date_display = f"{start_dt.strftime('%Y-%m-%d')}"
    
    # Prepare tasks for parallel execution
    tasks = []
    for db, tables in tables_by_db.items():
        for table in tables:
            tasks.append((db, table))
    
    # Execute tasks in parallel
    max_workers = min(len(tasks), db_config.pool_size, 20)  # Cap at 20 workers
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(process_table_data, db, table, start_date, end_date): (db, table)
            for db, table in tasks
        }
        
        for future in as_completed(future_to_task):
            db, table = future_to_task[future]
            try:
                results = future.result(timeout=30)  # 30 second timeout per task
                if results:
                    with lock:
                        all_records.extend(results)
                        for record in results:
                            active_databases.add(record.customer.lower())
                            models.add(record.model)
                            stations.add(record.station)
            except Exception as e:
                logger.error(f"Error processing {db}.{table}: {e}")
    
    return (
        all_records,
        date_display,
        sorted(active_databases),
        sorted(models),
        sorted(stations)
    )

def apply_filters(records: List[ProductionRecord], filters: Dict[str, Optional[str]]) -> List[ProductionRecord]:
    """Apply filters to production records"""
    filtered = records
    
    if filters.get("customer"):
        filtered = [r for r in filtered if r.customer.lower() == filters["customer"].lower()]
    if filters.get("model"):
        filtered = [r for r in filtered if r.model.lower() == filters["model"].lower()]
    if filters.get("station"):
        filtered = [r for r in filtered if r.station.lower() == filters["station"].lower()]
    
    return filtered

def get_operator_statistics(records: List[ProductionRecord], operator_name: str) -> Dict:
    """Calculate statistics for a specific operator"""
    filtered_records = [r for r in records if r.operator == operator_name]
    
    all_serials = []
    total_output = 0
    total_duration = 0
    all_durations = []
    
    for record in filtered_records:
        all_serials.extend(record.serial_nums)
        total_output += record.output
        if record.individual_durations:
            all_durations.extend(record.individual_durations)
        if record.duration_hours:
            total_duration += record.duration_hours
    
    # Calculate mode of cycle times if available
    mode_cycle_time = None
    if all_durations:
        try:
            rounded_durations = np.round(all_durations, 2)
            mode_result = stats.mode(rounded_durations, keepdims=False)
            mode_cycle_time = float(mode_result.mode)
        except Exception as e:
            logger.warning(f"Could not calculate mode for operator {operator_name}: {e}")
    
    return {
        "serials": all_serials,
        "total_output": total_output,
        "total_duration_hours": round(total_duration, 2),
        "mode_cycle_time": mode_cycle_time,
        "stations_count": len(set(f"{r.customer}-{r.model}-{r.station}" for r in filtered_records)),
        "records": filtered_records
    }