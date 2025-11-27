"""Business logic and data processing services - Optimized version with Active Filter"""
from typing import Optional, List, Dict, Tuple, Set
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import numpy as np
from scipy import stats
from datetime import datetime, timedelta
import time

from models import ProductionRecord
from database import get_db_connection, lock
from data_processor import ProductionDataProcessor
from utils import get_production_start_time, get_production_date_range
from config import db_config

logger = logging.getLogger(__name__)

# UTC offset constant: 7 hours and 15 minutes
UTC_OFFSET_HOURS = 0

def get_active_operators(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Set[str]:
    """
    Get set of operators who have entries in break_logs for the given date range
    
    Args:
        start_date: Start date string in 'YYYY-MM-DD' format
        end_date: End date string in 'YYYY-MM-DD' format
    
    Returns:
        Set of operator names who are active (have break_logs entries)
    """
    try:
        with get_db_connection() as cursor:
            # Determine date range
            if start_date and end_date:
                start_dt, end_dt = get_production_date_range(start_date, end_date)
            else:
                start_dt, end_dt = get_production_start_time()
            
            # Adjust for UTC offset
            adjusted_start_dt = start_dt - timedelta(hours=UTC_OFFSET_HOURS)
            adjusted_end_dt = end_dt - timedelta(hours=UTC_OFFSET_HOURS)
            
            # Query to get unique operators from break_logs
            query = """
                SELECT DISTINCT operator_en
                FROM projectsdb.break_logs
                WHERE timestamp BETWEEN %s AND %s
            """
            
            cursor.execute(query, (adjusted_start_dt, adjusted_end_dt))
            results = cursor.fetchall()
            
            active_operators = {row[0] for row in results if row[0]}
            logger.info(f"Found {len(active_operators)} active operators with break_logs")
            
            return active_operators
            
    except Exception as e:
        logger.error(f"Error fetching active operators: {e}")
        return set()

def process_table_data(
    database: str, 
    table: str, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None
) -> List[ProductionRecord]:
    """Process data from a single table using break_logs as primary source"""
    start_time = time.time()
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

            # Parse table name for model and station
            parts = table.split('_', 1)
            model = parts[0]
            station = parts[1] if len(parts) == 2 else ''

            # Query to get operators with their output from production tables
            query = f"""
                SELECT 
                    operator_en,
                    COUNT(DISTINCT serial_num) as current_output,
                    MIN(`{date_column}`) as first_record_time,
                    MAX(`{date_column}`) as last_record_time
                FROM `{database}`.`{table}`
                WHERE {where_clause} AND `status` = 1
                GROUP BY operator_en
                HAVING current_output > 0
            """
            
            cursor.execute(query, params)
            rows = cursor.fetchall()

            if not rows:
                return []

            results = []
            
            # Adjust date range for break_logs UTC query
            adjusted_start_dt = start_dt - timedelta(hours=UTC_OFFSET_HOURS)
            adjusted_end_dt = end_dt - timedelta(hours=UTC_OFFSET_HOURS)
            
            # OPTIMIZATION: Batch fetch all break_logs for all operators at once
            operator_list = [row[0] for row in rows]
            
            # Single query for all operators in this table
            placeholders = ','.join(['%s'] * len(operator_list))
            batch_break_logs_query = f"""
                SELECT operator_en, timestamp, action_type
                FROM projectsdb.break_logs
                WHERE operator_en IN ({placeholders})
                AND timestamp BETWEEN %s AND %s
                ORDER BY operator_en, timestamp ASC
            """
            cursor.execute(batch_break_logs_query, tuple(operator_list) + (adjusted_start_dt, adjusted_end_dt))
            all_logs = cursor.fetchall()
            
            # Group logs by operator for faster lookup
            logs_by_operator = {}
            for operator_en, timestamp, action_type in all_logs:
                if operator_en not in logs_by_operator:
                    logs_by_operator[operator_en] = []
                logs_by_operator[operator_en].append((timestamp, action_type))
            
            # OPTIMIZATION: Batch fetch all serial numbers
            serial_nums_by_operator = {}
            for operator_en in operator_list:
                detail_query = f"""
                    SELECT serial_num
                    FROM `{database}`.`{table}`
                    WHERE {where_clause} AND operator_en = %s AND `status` = 1
                    ORDER BY `{date_column}`
                """
                detail_params = params + (operator_en,)
                cursor.execute(detail_query, detail_params)
                records = cursor.fetchall()
                serial_nums_by_operator[operator_en] = [r[0] for r in records]
            
            for operator_en, current_output, first_record_time, last_record_time in rows:
                # Get pre-fetched logs and serial numbers
                logs = logs_by_operator.get(operator_en, [])
                serial_nums = serial_nums_by_operator.get(operator_en, [])

                # --- Process break_logs to calculate cycle time and work periods ---
                try:
                    # Check if operator has break logs
                    if not logs:
                        # NO BREAK LOGS - Use production timestamps as fallback
                        logger.debug(f"No break_logs for {operator_en} at {model}_{station} - using production data fallback")
                        
                        actual_start_time = first_record_time
                        actual_end_time = last_record_time
                        
                        # Calculate duration from production records
                        total_duration = (last_record_time - first_record_time).total_seconds()
                        
                        # Simple cycle time calculation: total time / output
                        if current_output > 0 and total_duration > 0:
                            cycle_time = total_duration / current_output
                        else:
                            cycle_time = 0
                        
                        duration_hours = total_duration / 3600 if total_duration > 0 else 0
                        individual_durations = [cycle_time] if cycle_time > 0 else []
                        
                    else:
                        # HAS BREAK LOGS - Use break_logs for accurate timing
                        # Filter break_logs to only those within the station's production timeframe
                        station_start_buffer = first_record_time - timedelta(minutes=30)
                        station_end_buffer = last_record_time + timedelta(minutes=30)
                        
                        # Convert to UTC time for comparison with break_logs
                        station_start_utc = station_start_buffer - timedelta(hours=UTC_OFFSET_HOURS)
                        station_end_utc = station_end_buffer - timedelta(hours=UTC_OFFSET_HOURS)
                        
                        # Filter logs to this station's timeframe
                        relevant_logs = [
                            (ts, action) for ts, action in logs 
                            if station_start_utc <= ts <= station_end_utc
                        ]
                        
                        if not relevant_logs:
                            # Break logs exist but none in this station's timeframe
                            logger.debug(f"No relevant break_logs for {operator_en} at {model}_{station} - using production data")
                            actual_start_time = first_record_time
                            actual_end_time = last_record_time
                            total_duration = (last_record_time - first_record_time).total_seconds()
                            cycle_time = total_duration / current_output if current_output > 0 and total_duration > 0 else 0
                            duration_hours = total_duration / 3600 if total_duration > 0 else 0
                            individual_durations = [cycle_time] if cycle_time > 0 else []
                        else:
                            # Process break logs normally
                            total_active_seconds = 0
                            start_time_log = None
                            work_sessions = []
                            last_stop_time = None
                            first_start_time = None
                            
                            for ts, action in relevant_logs:
                                # Apply UTC offset to convert timestamps to local time
                                local_ts = ts + timedelta(hours=UTC_OFFSET_HOURS)
                                
                                if action.lower() == "start":
                                    start_time_log = local_ts
                                    if first_start_time is None:
                                        first_start_time = local_ts
                                elif action.lower() == "stop" and start_time_log:
                                    session_duration = (local_ts - start_time_log).total_seconds()
                                    if session_duration > 0:
                                        total_active_seconds += session_duration
                                        work_sessions.append({
                                            'start': start_time_log,
                                            'stop': local_ts,
                                            'duration': session_duration
                                        })
                                    last_stop_time = local_ts
                                    start_time_log = None

                            actual_start_time = first_start_time if first_start_time else first_record_time

                            # Handle case where operator started but hasn't stopped yet
                            if start_time_log:
                                current_stop = last_record_time
                                session_duration = (current_stop - start_time_log).total_seconds()
                                if session_duration > 0:
                                    total_active_seconds += session_duration
                                    work_sessions.append({
                                        'start': start_time_log,
                                        'stop': current_stop,
                                        'duration': session_duration
                                    })
                                actual_end_time = current_stop
                            else:
                                actual_end_time = last_stop_time if last_stop_time else last_record_time

                            # Compute average cycle time
                            if current_output > 0 and total_active_seconds > 0:
                                cycle_time = total_active_seconds / current_output
                                logger.debug(f"Operator {operator_en} at {model}_{station}: {len(work_sessions)} sessions, "
                                          f"{total_active_seconds:.0f}s active, {current_output} output, "
                                          f"cycle time: {cycle_time:.2f}s")
                            else:
                                cycle_time = 0

                            individual_durations = [cycle_time] if cycle_time > 0 else []
                            duration_hours = total_active_seconds / 3600 if total_active_seconds > 0 else 0

                except Exception as err:
                    logger.error(f"Could not compute cycle time for {operator_en} at {model}_{station}: {err}")
                    cycle_time = 0
                    individual_durations = []
                    actual_start_time = first_record_time
                    actual_end_time = last_record_time
                    duration_hours = 0

                # Get target times
                target_time = ProductionDataProcessor.get_target_time(cursor, database, model, station)

                # Determine status based on cycle time vs target
                if target_time is None:
                    if cycle_time and cycle_time > 0:
                        status = "ON TARGET"
                    else:
                        status = "NO TARGET"
                else:
                    orange_threshold = target_time + (target_time * 0.2)
                    
                    if cycle_time <= target_time:
                        status = "ON TARGET"
                    elif cycle_time <= orange_threshold:
                        status = "ORANGE TARGET"
                    else:
                        status = "BELOW TARGET"

                record = ProductionRecord(
                    customer=database,
                    model=model,
                    station=station,
                    operator=operator_en,
                    output=current_output,
                    target_time=target_time,
                    cycle_time=cycle_time,
                    start_time=actual_start_time,
                    end_time=actual_end_time,
                    status=status,
                    serial_nums=serial_nums,
                    duration_hours=duration_hours,
                    individual_durations=individual_durations
                )
                results.append(record)

            elapsed = time.time() - start_time
            if elapsed > 2.0:
                logger.warning(f"Slow table processing: {database}.{table} took {elapsed:.2f}s")
            
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

# OPTIMIZATION: Add simple in-memory cache for production data
_production_data_cache = {}
_cache_timeout = 60  # 60 seconds

def fetch_production_data(
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None,
    use_cache: bool = True
) -> Tuple[List[ProductionRecord], str, List[str], List[str], List[str]]:
    """Fetch all production data with parallel processing and caching"""
    
    # Check cache
    cache_key = f"{start_date}_{end_date}"
    if use_cache and cache_key in _production_data_cache:
        cached_data, cached_time = _production_data_cache[cache_key]
        if time.time() - cached_time < _cache_timeout:
            logger.info(f"Using cached production data for {cache_key}")
            return cached_data
    
    start_time = time.time()
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
    
    logger.info(f"Processing {len(tasks)} tables with parallel execution")
    
    # Execute tasks in parallel
    max_workers = min(len(tasks), db_config.pool_size, 20)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(process_table_data, db, table, start_date, end_date): (db, table)
            for db, table in tasks
        }
        
        for future in as_completed(future_to_task):
            db, table = future_to_task[future]
            try:
                results = future.result(timeout=45)  # Increased timeout
                if results:
                    with lock:
                        all_records.extend(results)
                        for record in results:
                            active_databases.add(record.customer.lower())
                            models.add(record.model)
                            stations.add(record.station)
            except Exception as e:
                logger.error(f"Error processing {db}.{table}: {e}")
    
    result = (
        all_records,
        date_display,
        sorted(active_databases),
        sorted(models),
        sorted(stations)
    )
    
    # Cache the result
    if use_cache:
        _production_data_cache[cache_key] = (result, time.time())
    
    elapsed = time.time() - start_time
    logger.info(f"Fetched production data in {elapsed:.2f}s - {len(all_records)} records")
    
    return result

def apply_filters(
    records: List[ProductionRecord], 
    filters: Dict[str, Optional[str]],
    active_operators: Optional[Set[str]] = None
) -> List[ProductionRecord]:
    """Apply filters to production records including active filter"""
    filtered = records
    
    if filters.get("customer"):
        filtered = [r for r in filtered if r.customer.lower() == filters["customer"].lower()]
    if filters.get("model"):
        filtered = [r for r in filtered if r.model.lower() == filters["model"].lower()]
    if filters.get("station"):
        filtered = [r for r in filtered if r.station.lower() == filters["station"].lower()]
    
    # Apply active filter if provided
    if active_operators is not None:
        filtered = [r for r in filtered if r.operator in active_operators]
    
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

def clear_production_cache():
    """Clear the production data cache - useful for forced refresh"""
    global _production_data_cache
    _production_data_cache = {}
    logger.info("Production data cache cleared")