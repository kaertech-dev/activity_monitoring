"""Business logic and data processing services - Refactored with Employee Names"""
from typing import Optional, List, Dict, Tuple, Set
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import datetime, timedelta
import time

from models import ProductionRecord
from database import get_db_connection, lock
from data_processor import ProductionDataProcessor
from utils import get_production_start_time, get_production_date_range
from config import db_config

logger = logging.getLogger(__name__)

UTC_OFFSET_HOURS = 0


def get_operator_name(cursor, operator_en: str) -> str:
    """
    Get employee name from operators.main table
    
    Args:
        cursor: Database cursor
        operator_en: Operator code (e.g., 'KE0447')
    
    Returns:
        Employee name or operator code if not found
    """
    try:
        cursor.execute("""
            SELECT employee_name 
            FROM operators.main 
            WHERE operator_en = %s
            LIMIT 1
        """, (operator_en,))
        result = cursor.fetchone()
        return result[0] if result else operator_en
    except Exception as e:
        logger.warning(f"Could not fetch name for {operator_en}: {e}")
        return operator_en


def get_operator_names_batch(cursor, operator_codes: List[str]) -> Dict[str, str]:
    """
    Get employee names for multiple operators in one query
    
    Args:
        cursor: Database cursor
        operator_codes: List of operator codes
    
    Returns:
        Dictionary mapping operator_en to employee_name
    """
    if not operator_codes:
        return {}
    
    try:
        placeholders = ','.join(['%s'] * len(operator_codes))
        cursor.execute(f"""
            SELECT operator_en, employee_name 
            FROM operators.main 
            WHERE operator_en IN ({placeholders})
        """, tuple(operator_codes))
        
        # Create mapping dict, use operator_en as fallback if name not found
        name_map = {code: code for code in operator_codes}
        for operator_en, employee_name in cursor.fetchall():
            if employee_name:
                name_map[operator_en] = employee_name
        
        return name_map
    except Exception as e:
        logger.error(f"Error fetching operator names: {e}")
        # Return dict with codes as fallback
        return {code: code for code in operator_codes}


def get_active_operators(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Set[str]:
    """Get operators who have break_logs entries (returns operator codes)"""
    try:
        with get_db_connection() as cursor:
            if start_date and end_date:
                start_dt, end_dt = get_production_date_range(start_date, end_date)
            else:
                start_dt, end_dt = get_production_start_time()
            
            adjusted_start = start_dt - timedelta(hours=UTC_OFFSET_HOURS)
            adjusted_end = end_dt - timedelta(hours=UTC_OFFSET_HOURS)
            
            cursor.execute("""
                SELECT DISTINCT operator_en
                FROM projectsdb.break_logs
                WHERE timestamp BETWEEN %s AND %s
            """, (adjusted_start, adjusted_end))
            
            return {row[0] for row in cursor.fetchall() if row[0]}
    except Exception as e:
        logger.error(f"Error fetching active operators: {e}")
        return set()


def process_table_data(
    database: str, 
    table: str, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None
) -> List[ProductionRecord]:
    """Process data from a single table"""
    try:
        with get_db_connection() as cursor:
            columns_info = ProductionDataProcessor.get_table_columns(cursor, database, table)
            
            if not ProductionDataProcessor.validate_required_columns(columns_info):
                return []

            date_column = ProductionDataProcessor.find_date_column(columns_info)
            if not date_column:
                return []

            # Get date range
            if start_date and end_date:
                start_dt, end_dt = get_production_date_range(start_date, end_date)
            else:
                start_dt, end_dt = get_production_start_time()

            # Parse table name
            parts = table.split('_', 1)
            model = parts[0]
            station = parts[1] if len(parts) == 2 else ''

            # Query production data
            query = f"""
                SELECT 
                    operator_en,
                    COUNT(DISTINCT serial_num) as output,
                    MIN(`{date_column}`) as first_time,
                    MAX(`{date_column}`) as last_time
                FROM `{database}`.`{table}`
                WHERE `{date_column}` BETWEEN %s AND %s AND `status` = 1
                GROUP BY operator_en
                HAVING output > 0
            """
            
            cursor.execute(query, (start_dt, end_dt))
            rows = cursor.fetchall()

            if not rows:
                return []

            # Batch fetch operator names
            operator_codes = [row[0] for row in rows]
            operator_names = get_operator_names_batch(cursor, operator_codes)

            results = []
            adjusted_start = start_dt - timedelta(hours=UTC_OFFSET_HOURS)
            adjusted_end = end_dt - timedelta(hours=UTC_OFFSET_HOURS)
            
            # Batch fetch break logs
            placeholders = ','.join(['%s'] * len(operator_codes))
            
            cursor.execute(f"""
                SELECT operator_en, timestamp, action_type
                FROM projectsdb.break_logs
                WHERE operator_en IN ({placeholders})
                AND timestamp BETWEEN %s AND %s
                ORDER BY operator_en, timestamp ASC
            """, tuple(operator_codes) + (adjusted_start, adjusted_end))
            
            # Group logs by operator
            logs_by_operator = {}
            for operator_en, timestamp, action_type in cursor.fetchall():
                if operator_en not in logs_by_operator:
                    logs_by_operator[operator_en] = []
                logs_by_operator[operator_en].append((timestamp, action_type))
            
            # Process each operator
            for operator_en, output, first_time, last_time in rows:
                logs = logs_by_operator.get(operator_en, [])
                
                # Fetch serial numbers
                cursor.execute(f"""
                    SELECT serial_num
                    FROM `{database}`.`{table}`
                    WHERE `{date_column}` BETWEEN %s AND %s 
                    AND operator_en = %s AND `status` = 1
                    ORDER BY `{date_column}`
                """, (start_dt, end_dt, operator_en))
                serial_nums = [r[0] for r in cursor.fetchall()]

                # Calculate cycle time
                cycle_time, start_time, end_time = calculate_cycle_time(
                    logs, first_time, last_time, output, model, station
                )

                # Get target and status
                target_time = ProductionDataProcessor.get_target_time(cursor, database, model, station)
                status = determine_status(cycle_time, target_time)

                # Get employee name
                employee_name = operator_names.get(operator_en, operator_en)

                results.append(ProductionRecord(
                    customer=database,
                    model=model,
                    station=station,
                    operator=employee_name,  # Use employee name instead of code
                    operator_code=operator_en,  # Keep code for reference
                    output=output,
                    target_time=target_time,
                    cycle_time=cycle_time,
                    start_time=start_time,
                    end_time=end_time,
                    status=status,
                    serial_nums=serial_nums,
                    duration_hours=(end_time - start_time).total_seconds() / 3600 if start_time and end_time else 0,
                    individual_durations=[cycle_time] if cycle_time > 0 else []
                ))
            
            return results

    except Exception as e:
        logger.error(f"Error processing {database}.{table}: {e}")
        return []


def calculate_cycle_time(logs, first_time, last_time, output, model, station):
    """Calculate cycle time from break logs or production data"""
    if not logs:
        # No break logs - use production data
        duration = (last_time - first_time).total_seconds()
        cycle_time = duration / output if output > 0 and duration > 0 else 0
        return cycle_time, first_time, last_time
    
    # Filter logs to production timeframe
    station_start = first_time - timedelta(minutes=30) - timedelta(hours=UTC_OFFSET_HOURS)
    station_end = last_time + timedelta(minutes=30) - timedelta(hours=UTC_OFFSET_HOURS)
    relevant_logs = [(ts, action) for ts, action in logs if station_start <= ts <= station_end]
    
    if not relevant_logs:
        duration = (last_time - first_time).total_seconds()
        cycle_time = duration / output if output > 0 and duration > 0 else 0
        return cycle_time, first_time, last_time
    
    # Process break logs
    total_active = 0
    start_log = None
    first_start = None
    last_stop = None
    
    for ts, action in relevant_logs:
        local_ts = ts + timedelta(hours=UTC_OFFSET_HOURS)
        
        if action.lower() == "start":
            start_log = local_ts
            if first_start is None:
                first_start = local_ts
        elif action.lower() == "stop" and start_log:
            duration = (local_ts - start_log).total_seconds()
            if duration > 0:
                total_active += duration
            last_stop = local_ts
            start_log = None
    
    # Handle unclosed session
    if start_log:
        duration = (last_time - start_log).total_seconds()
        if duration > 0:
            total_active += duration
        last_stop = last_time
    
    cycle_time = total_active / output if output > 0 and total_active > 0 else 0
    start_time = first_start if first_start else first_time
    end_time = last_stop if last_stop else last_time
    
    return cycle_time, start_time, end_time


def determine_status(cycle_time, target_time):
    """Determine production status"""
    if target_time is None:
        return "ON TARGET" if cycle_time > 0 else "NO TARGET"
    
    orange_threshold = target_time * 1.2
    if cycle_time <= target_time:
        return "ON TARGET"
    elif cycle_time <= orange_threshold:
        return "ORANGE TARGET"
    else:
        return "BELOW TARGET"


@lru_cache(maxsize=128)
def get_databases_and_tables() -> Tuple[List[str], Dict[str, List[str]]]:
    """Get all databases and their tables (cached)"""
    try:
        with get_db_connection() as cursor:
            cursor.execute("SHOW DATABASES")
            databases = [db[0] for db in cursor.fetchall() 
                        if db[0] not in db_config.hidden_databases]
            
            tables_by_db = {}
            for db in databases:
                try:
                    cursor.execute(f"SHOW TABLES FROM `{db}`")
                    tables_by_db[db] = [tbl[0] for tbl in cursor.fetchall()]
                except Exception as e:
                    logger.warning(f"Could not access {db}: {e}")
                    tables_by_db[db] = []
            
            return databases, tables_by_db
    except Exception as e:
        logger.error(f"Error getting databases: {e}")
        return [], {}


_cache = {}
_cache_timeout = 60

def fetch_production_data(
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None,
    use_cache: bool = True
) -> Tuple[List[ProductionRecord], str, List[str], List[str], List[str]]:
    """Fetch all production data"""
    cache_key = f"{start_date}_{end_date}"
    
    if use_cache and cache_key in _cache:
        data, cached_time = _cache[cache_key]
        if time.time() - cached_time < _cache_timeout:
            return data
    
    databases, tables_by_db = get_databases_and_tables()
    all_records = []
    active_databases = set()
    models = set()
    stations = set()
    
    # Prepare tasks
    tasks = [(db, table) for db, tables in tables_by_db.items() for table in tables]
    
    # Process in parallel
    max_workers = min(len(tasks), db_config.pool_size, 20)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(process_table_data, db, table, start_date, end_date): (db, table)
            for db, table in tasks
        }
        
        for future in as_completed(future_to_task):
            try:
                results = future.result(timeout=45)
                if results:
                    with lock:
                        all_records.extend(results)
                        for record in results:
                            active_databases.add(record.customer.lower())
                            models.add(record.model)
                            stations.add(record.station)
            except Exception as e:
                db, table = future_to_task[future]
                logger.error(f"Error processing {db}.{table}: {e}")
    
    # Date display
    if start_date and end_date:
        date_display = f"{start_date} → {end_date}"
    else:
        start_dt, _ = get_production_start_time()
        date_display = start_dt.strftime('%Y-%m-%d')
    
    result = (all_records, date_display, sorted(active_databases), sorted(models), sorted(stations))
    
    if use_cache:
        _cache[cache_key] = (result, time.time())
    
    return result


def apply_filters(
    records: List[ProductionRecord], 
    filters: Dict[str, Optional[str]],
    active_operators: Optional[Set[str]] = None
) -> List[ProductionRecord]:
    """Apply filters to records"""
    filtered = records
    
    if filters.get("customer"):
        filtered = [r for r in filtered if r.customer.lower() == filters["customer"].lower()]
    if filters.get("model"):
        filtered = [r for r in filtered if r.model.lower() == filters["model"].lower()]
    if filters.get("station"):
        filtered = [r for r in filtered if r.station.lower() == filters["station"].lower()]
    if active_operators is not None:
        # Filter by operator_code since active_operators contains codes
        filtered = [r for r in filtered if r.operator_code in active_operators]
    
    return filtered


def get_operator_statistics(records: List[ProductionRecord], operator_name: str) -> Dict:
    """Calculate statistics for a specific operator (by name or code)"""
    # Try to match by name first, then by code
    filtered = [r for r in records if r.operator == operator_name or r.operator_code == operator_name]
    
    all_serials = []
    total_output = 0
    total_duration = 0
    
    for record in filtered:
        all_serials.extend(record.serial_nums)
        total_output += record.output
        if record.duration_hours:
            total_duration += record.duration_hours
    
    return {
        "serials": all_serials,
        "total_output": total_output,
        "total_duration_hours": round(total_duration, 2),
        "mode_cycle_time": None,
        "stations_count": len(set(f"{r.customer}-{r.model}-{r.station}" for r in filtered)),
        "records": filtered
    }