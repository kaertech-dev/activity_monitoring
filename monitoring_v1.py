from mysql.connector.pooling import MySQLConnectionPool
from scipy import stats
from fastapi import FastAPI, Request, Query, HTTPException, Depends
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from contextlib import contextmanager
import threading
import logging
import os
from functools import lru_cache
import numpy as np
import csv
import io
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Production Monitoring System", version="2.0.0")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@dataclass
class ProductionRecord:
    """Data class for production records with enhanced statistics"""
    customer: str
    model: str
    station: str
    operator: str
    output: int
    target_time: Optional[float]
    cycle_time: float
    start_time: datetime
    end_time: datetime
    status: str
    serial_nums: List[str]
    duration_hours: Optional[float] = None
    individual_durations: List[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'Customer': self.customer.upper(),
            'Model': self.model.upper(),
            'Station': self.station.upper(),
            'Operator': self.operator,
            'Output': self.output,
            'Target(s)': self.target_time,
            'Cycle Time(s)': f"{self.cycle_time:.2f}" if self.cycle_time != 0 else '-',
            'Start Time': self.start_time.strftime('%H:%M:%S') if self.start_time else None,
            'End time': self.end_time.strftime('%H:%M:%S') if self.end_time else None,
            'Status': self.status,
            'serial_num': self.serial_nums
        }

class DatabaseConfig:
    """Database configuration management"""
    def __init__(self):
        self.config = {
            'host': os.getenv('DB_HOST', '192.168.1.38'),
            'user': os.getenv('DB_USER', 'labeling'),
            'password': os.getenv('DB_PASSWORD', 'labeling'),
            'autocommit': True,
            'use_unicode': True,
            'charset': 'utf8mb4'
        }
        self.pool_size = int(os.getenv('DB_POOL_SIZE', '15'))
        self.hidden_databases = ['smt', 'onanofflimited']

db_config = DatabaseConfig()

# Initialize connection pool
try:
    POOL = MySQLConnectionPool(
        pool_name="production_monitoring_pool_v2",
        pool_size=db_config.pool_size,
        **db_config.config
    )
    logger.info(f"Database connection pool initialized with {db_config.pool_size} connections")
except Exception as e:
    logger.error(f"Failed to initialize database pool: {e}")
    raise

# Thread lock for safe concurrent operations
lock = threading.Lock()

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    cursor = None
    try:
        conn = POOL.get_connection()
        cursor = conn.cursor(buffered=True)
        yield cursor
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_production_start_time(target_date: str = None) -> Tuple[datetime, datetime]:
    """
    Get production start and end times for a given date.
    Production day starts at 7:00 AM and ends at 6:59:59 AM next day.
    
    Args:
        target_date: Date string in 'YYYY-MM-DD' format. If None, uses current date.
    
    Returns:
        Tuple of (start_datetime, end_datetime) for the production day
    """
    if target_date:
        base_date = datetime.strptime(target_date, '%Y-%m-%d').date()
    else:
        current_time = datetime.now()
        # If current time is before 7 AM, consider it part of previous production day
        if current_time.hour < 7:
            base_date = current_time.date() - timedelta(days=0)
        else:
            base_date = current_time.date()
    
    # Production day starts at 7:00 AM
    start_time = datetime.combine(base_date, datetime.min.time().replace(hour=7, minute=0, second=0))
    # Production day ends at 6:59:59 AM next day
    end_time = start_time + timedelta(days=1) - timedelta(seconds=1)
    
    return start_time, end_time

def get_production_date_range(start_date: str = None, end_date: str = None) -> Tuple[datetime, datetime]:
    """
    Get production date range considering 7 AM start times.
    
    Args:
        start_date: Start date string in 'YYYY-MM-DD' format
        end_date: End date string in 'YYYY-MM-DD' format
    
    Returns:
        Tuple of (start_datetime, end_datetime) for the production period
    """
    if start_date and end_date:
        # Start from 7:00 AM of start_date
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').replace(hour=7, minute=0, second=0)
        # End at 6:59:59 AM of the day after end_date
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=6, minute=59, second=59) + timedelta(days=1)
        return start_dt, end_dt
    else:
        # Default to current production day
        return get_production_start_time()

class ProductionDataProcessor:
    """Enhanced production data processing with statistical analysis"""
    
    @staticmethod
    def get_table_columns(cursor, database: str, table: str) -> List[tuple]:
        """Get column information for a table"""
        cursor.execute(f"USE `{database}`")
        cursor.execute(f"DESCRIBE `{table}`")
        return cursor.fetchall()
    
    @staticmethod
    def find_date_column(columns_info: List[tuple]) -> Optional[str]:
        """Find the date/time column in the table using both name and type"""
        for col_info in columns_info:
            col_name = col_info[0].lower()
            col_type = col_info[1].lower()
            if (('date' in col_name or 'time' in col_name) and 
                ('date' in col_type or 'timestamp' in col_type)):
                return col_info[0]
        return None
    
    @staticmethod
    def validate_required_columns(columns_info: List[tuple]) -> bool:
        """Check if table has required columns"""
        column_names = {col[0] for col in columns_info}
        required = {"operator_en", "serial_num", "status"}
        return required.issubset(column_names)
    
    @staticmethod
    def get_target_time(cursor, database: str, model: str, station: str) -> Optional[float]:
        """Get target time from production_plan table"""
        try:
            cursor.execute("""
                SELECT process_time FROM production_plan.target_time
                WHERE customer = %s AND model = %s AND station = %s
                LIMIT 1
            """, (database, model, station))
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.warning(f"Could not fetch target time for {database}-{model}-{station}: {e}")
            return None
    
    @staticmethod
    def calculate_cycle_statistics(timestamps: List[datetime], output_count: int) -> Dict[str, float]:
        """Calculate various cycle time statistics"""
        if len(timestamps) < 2:
            return {"avg_cycle_time": 0, "durations": []}
        
        # Calculate durations between consecutive records
        durations = []
        for i in range(1, len(timestamps)):
            diff = (timestamps[i] - timestamps[i-1]).total_seconds()
            if diff > 0:
                durations.append(diff)
        
        if not durations:
            total_duration = (timestamps[-1] - timestamps[0]).total_seconds()
            avg_cycle_time = total_duration / output_count if output_count > 0 else 0
            return {"avg_cycle_time": avg_cycle_time, "durations": []}
        
        # Calculate average of 3 shortest durations if available
        sorted_durations = sorted(durations)
        if len(sorted_durations) >= 3:
            avg_3_shortest = sum(sorted_durations[:3]) / 3
        else:
            avg_3_shortest = sum(sorted_durations) / len(sorted_durations)
        
        return {
            "avg_cycle_time": avg_3_shortest,
            "durations": durations,
            "total_duration": (timestamps[-1] - timestamps[0]).total_seconds()
        }

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
                cycle_stats = ProductionDataProcessor.calculate_cycle_statistics(timestamps, current_output)
                cycle_time = cycle_stats["avg_cycle_time"]
                individual_durations = cycle_stats.get("durations", [])

                # Alternative calculation: total time / output count
                if start_time and end_time:
                    total_duration_calc = (end_time - start_time).total_seconds() / current_output
                    # Use the more conservative (higher) cycle time
                    cycle_time = max(cycle_time, total_duration_calc)

                # Get target time
                target_time = ProductionDataProcessor.get_target_time(cursor, database, model, station)

                # Determine status
                status = "NO TARGET"
                if target_time is not None:
                    orange_target = target_time * 0.2
                    if cycle_time <= target_time:
                        status = "ON TARGET"
                    elif cycle_time >= orange_target:
                        status = "ORANGE TARGET"
                    else:
                        status =  "BELOW TARGET"
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

# Dependency for common filtering
def get_filters(
    customer: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    station: Optional[str] = Query(None)
):
    return {"customer": customer, "model": model, "station": station}

def apply_filters(records: List[ProductionRecord], filters: Dict[str, Optional[str]]) -> List[ProductionRecord]:
    """Apply filters to production records"""
    filtered = records
    
    if filters["customer"]:
        filtered = [r for r in filtered if r.customer.lower() == filters["customer"].lower()]
    if filters["model"]:
        filtered = [r for r in filtered if r.model.lower() == filters["model"].lower()]
    if filters["station"]:
        filtered = [r for r in filtered if r.station.lower() == filters["station"].lower()]
    
    return filtered

@app.get("/", response_class=HTMLResponse)
async def show_operator_en_today(
    request: Request,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    filters: Dict[str, Optional[str]] = Depends(get_filters),
    submit_customer: Optional[str] = Query(None),
    submit_model: Optional[str] = Query(None),
    submit_station: Optional[str] = Query(None)
):
    """Main dashboard endpoint with 7 AM production day start"""
    try:
        records, date_str, active_databases, models, stations = fetch_production_data(start_date, end_date)
        filtered_records = apply_filters(records, filters)
        
        columns = ['Customer', 'Model', 'Station', 'Operator', 'Output', 'Cycle Time(s)', 'Target(s)', 'Start Time', 'End time', 'Status']
        rows = [tuple(record.to_dict()[col] for col in columns) for record in filtered_records]
        
        # Enhanced date display with production day info
        if start_date and end_date:
            current_date_display = f"{start_date} → {end_date}"
        else:
            start_dt, end_dt = get_production_start_time()
            current_date_display = f"{start_dt.strftime('%Y-%m-%d')}"# (from 7:00 AM)
        
        return templates.TemplateResponse("monitoring_v1.html", {
            "request": request,
            "rows": rows,
            "columns": columns,
            "current_date": current_date_display,
            "start_date": start_date,
            "end_date": end_date,
            "selected_db": filters["customer"],
            "selected_model": filters["model"],
            "selected_station": filters["station"],
            "db": active_databases,
            "models": models,
            "stations": stations,
            "total_records": len(filtered_records)
        })
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/operator_today", response_class=JSONResponse)
async def api_operator_today(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    filters: Dict[str, Optional[str]] = Depends(get_filters)
):
    """API endpoint for today's operator data with 7 AM start"""
    try:
        records, date_str, _, _, _ = fetch_production_data(start_date, end_date)
        filtered_records = apply_filters(records, filters)
        
        return {
            "date": date_str,
            "count": len(filtered_records),
            "records": [record.to_dict() for record in filtered_records],
            "filters_applied": {k: v for k, v in filters.items() if v},
            "production_day_info": "Production day starts at 7:00 AM"
        }
    except Exception as e:
        logger.error(f"API operator today error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch operator data")

@app.get("/operator/{operator_name}", response_class=HTMLResponse)
async def show_operator_activity(request: Request, operator_name: str):
    """Show specific operator activity with enhanced details and 7 AM start"""
    try:
        records, today_str, _, _, _ = fetch_production_data()
        filtered_records = [r for r in records if r.operator == operator_name]
        
        columns = ['Customer', 'Model', 'Station', 'Operator', 'Output',
                   'Cycle Time(s)', 'Target(s)', 'Start Time', 'End time', 'Status']
        rows = [tuple(record.to_dict()[col] for col in columns) for record in filtered_records]
        
        # Collect all serial numbers and calculate statistics
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
        
        operator_stats = {
            "total_output": total_output,
            "total_duration_hours": round(total_duration, 2),
            "mode_cycle_time": mode_cycle_time,
            "stations_count": len(set(f"{r.customer}-{r.model}-{r.station}" for r in filtered_records))
        }
        
        return templates.TemplateResponse("operator_activity.html", {
            "request": request,
            "rows": rows,
            "columns": columns,
            "current_date": today_str,
            "operator_name": operator_name,
            "serials": all_serials,
            "operator_stats": operator_stats
        })
    except Exception as e:
        logger.error(f"Operator activity error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch operator data")

@app.get("/api/operator_outputs/{operator_name}", response_class=JSONResponse)
async def api_operator_outputs(operator_name: str):
    """Get detailed outputs for a specific operator with 7 AM start"""
    try:
        records, _, _, _, _ = fetch_production_data()
        filtered_records = [r for r in records if r.operator == operator_name]

        outputs = []
        for record in filtered_records:
            cycle_time = record.cycle_time
            
            for idx, serial in enumerate(record.serial_nums, 1):
                outputs.append({
                    "serial_num": serial,
                    "cycle_time": f"{cycle_time:.2f}" if cycle_time > 0 else '-',
                    "operator": operator_name,
                    "order": idx,
                    "customer": record.customer,
                    "model": record.model,
                    "station": record.station
                })

        return {
            "operator": operator_name, 
            "outputs": outputs,
            "total_count": len(outputs),
            "production_day_info": "Production day starts at 7:00 AM"
        }
    except Exception as e:
        logger.error(f"Operator outputs API error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch operator outputs")

@app.get("/api/operator_summary", response_class=JSONResponse)
async def api_operator_summary():
    """Get comprehensive operator summary statistics with 7 AM start"""
    try:
        records, _, _, _, _ = fetch_production_data()
        
        summary = {}
        for record in records:
            operator = record.operator
            if operator not in summary:
                summary[operator] = {
                    "total_output": 0,
                    "avg_cycle_time": 0,
                    "stations_worked": set(),
                    "customers_served": set(),
                    "total_duration_hours": 0,
                    "cycle_times": []
                }
            
            summary[operator]["total_output"] += record.output
            summary[operator]["stations_worked"].add(f"{record.customer}-{record.model}-{record.station}")
            summary[operator]["customers_served"].add(record.customer)
            if record.duration_hours:
                summary[operator]["total_duration_hours"] += record.duration_hours
            if record.cycle_time > 0:
                summary[operator]["cycle_times"].append(record.cycle_time)
        
        # Calculate averages and convert sets to counts for JSON serialization
        for operator_data in summary.values():
            cycle_times = operator_data["cycle_times"]
            operator_data["avg_cycle_time"] = round(sum(cycle_times) / len(cycle_times), 2) if cycle_times else 0
            operator_data["stations_count"] = len(operator_data["stations_worked"])
            operator_data["customers_count"] = len(operator_data["customers_served"])
            operator_data["total_duration_hours"] = round(operator_data["total_duration_hours"], 2)
            
            # Remove non-serializable data
            del operator_data["stations_worked"]
            del operator_data["customers_served"]
            del operator_data["cycle_times"]
        
        return {
            "summary": summary, 
            "total_operators": len(summary),
            "generated_at": datetime.now().isoformat(),
            "production_day_info": "Production day starts at 7:00 AM"
        }
    except Exception as e:
        logger.error(f"Operator summary error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate operator summary")

@app.get("/api/get-models-stations", response_class=JSONResponse)
async def get_models_and_stations(
    customer: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    station: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None)
):
    """Get available models and stations based on actual data records with 7 AM logic"""
    try:
        # Fetch production data with the same date filters as the main dashboard
        records, _, _, _, _ = fetch_production_data(start_date, end_date)
        
        # Apply customer filter first if specified
        if customer:
            records = [r for r in records if r.customer.lower() == customer.lower()]
        
        # Extract unique models and stations from actual records
        available_models = set()
        available_stations = set()
        
        for record in records:
            # Apply additional filters if specified
            if model and record.model.lower() != model.lower():
                continue
            if station and record.station.lower() != station.lower():
                continue
            
            available_models.add(record.model)
            available_stations.add(record.station)
        
        # Convert to sorted lists
        models_list = sorted(list(available_models))
        stations_list = sorted(list(available_stations))
        
        return {
            "models": models_list,
            "stations": stations_list,
            "count": {
                "models": len(models_list),
                "stations": len(stations_list)
            },
            "customer_filter": customer,
            "model_filter": model,
            "station_filter": station,
            "date_filters": {
                "start_date": start_date,
                "end_date": end_date
            },
            "total_records": len(records) if customer else "N/A",
            "production_day_info": "Production day starts at 7:00 AM"
        }
        
    except Exception as e:
        logger.error(f"Data-based models/stations API error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch models and stations: {str(e)}")

from fastapi import HTTPException
from fastapi.responses import StreamingResponse

@app.get("/api/download_csv", response_class=StreamingResponse)
async def download_csv(
    date: str = None,
    week: str = None,
    month: str = None,
    start_date: str = None,
    end_date: str = None
):
    """Download production data as CSV with 7 AM production day logic"""
    try:
        # --- Validation helpers ---
        def validate_date(date_str: str) -> bool:
            if not date_str:
                return True
            try:
                datetime.strptime(date_str, '%Y-%m-%d')
                return True
            except ValueError:
                return False

        # Validate inputs
        if date and not validate_date(date):
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
        if start_date and not validate_date(start_date):
            raise HTTPException(status_code=400, detail="Invalid start_date format. Use YYYY-MM-DD")
        if end_date and not validate_date(end_date):
            raise HTTPException(status_code=400, detail="Invalid end_date format. Use YYYY-MM-DD")
        if week and not re.match(r'^\d{4}-W\d{1,2}$', week):
            raise HTTPException(status_code=400, detail="Invalid week format. Use YYYY-WNN")
        if month and not re.match(r'^\d{4}-\d{1,2}$', month):
            raise HTTPException(status_code=400, detail="Invalid month format. Use YYYY-MM")

        # --- Date logic ---
        if date:
            start_date = end_date = date
        elif week:
            year, week_num = map(int, week.split('-W'))
            # ISO-compliant week calculation
            start_date = datetime.fromisocalendar(year, week_num, 1).date().isoformat()
            end_date = datetime.fromisocalendar(year, week_num, 7).date().isoformat()
        elif month:
            year, month_num = map(int, month.split('-'))
            start_date = datetime(year, month_num, 1).date().isoformat()
            next_month = datetime(year, month_num, 28) + timedelta(days=4)
            end_date = datetime(next_month.year, next_month.month, 1) - timedelta(days=1)
            end_date = end_date.date().isoformat()

        # --- Fetch records ---
        records, _, _, _, _ = fetch_production_data(start_date=start_date, end_date=end_date)

        # --- Prepare CSV ---
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Customer', 'Model', 'Station', 'Operator', 'Output', 'Cycle Time(s)',
                         'Target(s)', 'Start Time', 'End time', 'Status', 'Serial Numbers'])
        writer.writerow(['# Production Day: Starts at 7:00 AM each day'])

        for record in records:
            row = [
                record.customer.upper(),
                record.model.upper(),
                record.station.upper(),
                record.operator,
                record.output,
                f"{record.cycle_time:.2f}" if record.cycle_time != 0 else '-',
                record.target_time,
                record.start_time.strftime('%H:%M:%S') if record.start_time else None,
                record.end_time.strftime('%H:%M:%S') if record.end_time else None,
                record.status,
                ', '.join(record.serial_nums) if isinstance(record.serial_nums, list) else record.serial_nums
            ]
            writer.writerow(row)

        output.seek(0)

        headers = {
            'Content-Disposition': f'attachment; filename="production_data.csv"'
        }

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers=headers
        )

    except Exception as e:
        logger.error(f"CSV download error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate CSV")

@app.get("/api/statistics", response_class=JSONResponse)
async def get_production_statistics(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    filters: Dict[str, Optional[str]] = Depends(get_filters)
):
    """Get comprehensive production statistics with 7 AM start logic"""
    try:
        records, date_str, _, _, _ = fetch_production_data(start_date, end_date)
        filtered_records = apply_filters(records, filters)
        
        if not filtered_records:
            return {"message": "No data found for the specified filters"}
        
        # Calculate overall statistics
        total_output = sum(r.output for r in filtered_records)
        total_operators = len(set(r.operator for r in filtered_records))
        total_stations = len(set(f"{r.customer}-{r.model}-{r.station}" for r in filtered_records))
        
        # Cycle time statistics
        cycle_times = [r.cycle_time for r in filtered_records if r.cycle_time > 0]
        avg_cycle_time = sum(cycle_times) / len(cycle_times) if cycle_times else 0
        
        # Status distribution
        status_counts = {}
        for record in filtered_records:
            status_counts[record.status] = status_counts.get(record.status, 0) + 1
        
        # Top performers
        operator_outputs = {}
        for record in filtered_records:
            operator_outputs[record.operator] = operator_outputs.get(record.operator, 0) + record.output
        
        top_operators = sorted(operator_outputs.items(), key=lambda x: x[1], reverse=True)[:5]
        
        return {
            "summary": {
                "total_output": total_output,
                "total_operators": total_operators,
                "total_stations": total_stations,
                "avg_cycle_time": round(avg_cycle_time, 2),
                "date_range": date_str
            },
            "status_distribution": status_counts,
            "top_operators": [{"operator": op, "output": out} for op, out in top_operators],
            "filters_applied": {k: v for k, v in filters.items() if v},
            "generated_at": datetime.now().isoformat(),
            "production_day_info": "Production day starts at 7:00 AM"
        }
    except Exception as e:
        logger.error(f"Statistics API error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate statistics")

@app.get("/api/production_day_info", response_class=JSONResponse)
async def get_production_day_info():
    """Get information about current production day timing"""
    try:
        current_time = datetime.now()
        start_dt, end_dt = get_production_start_time()
        
        # Determine if we're in current production day or next
        is_current_day = start_dt <= current_time <= end_dt
        
        # Calculate time remaining in current production day
        if is_current_day:
            time_remaining = end_dt - current_time
            status = "IN_PROGRESS"
        else:
            # If before 7 AM, show time until next production day starts
            next_start = start_dt + timedelta(days=1)
            time_remaining = next_start - current_time
            status = "WAITING_FOR_START"
        
        hours_remaining = time_remaining.total_seconds() / 3600
        
        return {
            "current_production_day": {
                "start": start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                "end": end_dt.strftime('%Y-%m-%d %H:%M:%S'),
                "status": status,
                "hours_remaining": round(hours_remaining, 2)
            },
            "current_time": current_time.strftime('%Y-%m-%d %H:%M:%S'),
            "is_production_hours": is_current_day,
            "production_day_info": "Production day runs from 7:00 AM to 6:59:59 AM next day"
        }
    except Exception as e:
        logger.error(f"Production day info error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get production day info")

@app.get("/health")
async def health_check():
    """Health check endpoint with production day info"""
    try:
        with get_db_connection() as cursor:
            cursor.execute("SELECT 1")
            
            # Add production day timing to health check
            start_dt, end_dt = get_production_start_time()
            current_time = datetime.now()
            is_production_hours = start_dt <= current_time <= end_dt
            
            return {
                "status": "healthy",
                "timestamp": current_time.isoformat(),
                "database": "connected",
                "pool_size": db_config.pool_size,
                "production_day": {
                    "current_start": start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    "current_end": end_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    "is_production_hours": is_production_hours,
                    "info": "Production day starts at 7:00 AM"
                }
            }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )
