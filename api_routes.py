# activity_monitoring/api_routes.py
"""API route handlers with Active Filter support"""
from fastapi import APIRouter, Query, HTTPException, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Optional, Dict
from datetime import datetime, timedelta
import logging
import csv
import io
import re

from services import fetch_production_data, apply_filters, get_operator_statistics, get_active_operators
from utils import get_production_start_time
from database import get_db_connection
from config import db_config

logger = logging.getLogger(__name__)
router = APIRouter()

# Dependency for common filtering
def get_filters(
    customer: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    station: Optional[str] = Query(None),
    active_filter: Optional[str] = Query("all")
):
    return {"customer": customer, "model": model, "station": station, "active_filter": active_filter}

@router.get("/api/operator_today", response_class=JSONResponse)
async def api_operator_today(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    filters: Dict[str, Optional[str]] = Depends(get_filters)
):
    """API endpoint for today's operator data with 7 AM start and active filter"""
    try:
        records, date_str, _, _, _ = fetch_production_data(start_date, end_date)
        
        # Get active operators if filter is set to "active"
        active_operators = None
        if filters.get("active_filter") == "active":
            active_operators = get_active_operators(start_date, end_date)
            logger.info(f"Active filter enabled in API: {len(active_operators)} active operators")
        
        filtered_records = apply_filters(records, filters, active_operators)
        
        response_data = {
            "date": date_str,
            "count": len(filtered_records),
            "records": [record.to_dict() for record in filtered_records],
            "filters_applied": {k: v for k, v in filters.items() if v},
            "production_day_info": "Production day starts at 7:00 AM"
        }
        
        if active_operators is not None:
            response_data["active_operators_count"] = len(active_operators)
        
        return response_data
    except Exception as e:
        logger.error(f"API operator today error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch operator data")

@router.get("/api/operator_outputs/{operator_name}", response_class=JSONResponse)
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

@router.get("/api/operator_summary", response_class=JSONResponse)
async def api_operator_summary(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    active_filter: Optional[str] = Query("all")
):
    """Get comprehensive operator summary statistics with 7 AM start and active filter"""
    try:
        records, _, _, _, _ = fetch_production_data(start_date, end_date)
        
        # Get active operators if filter is enabled
        active_operators = None
        if active_filter == "active":
            active_operators = get_active_operators(start_date, end_date)
            records = [r for r in records if r.operator in active_operators]
        
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
        
        response_data = {
            "summary": summary, 
            "total_operators": len(summary),
            "generated_at": datetime.now().isoformat(),
            "production_day_info": "Production day starts at 7:00 AM",
            "active_filter": active_filter
        }
        
        if active_operators is not None:
            response_data["active_operators_count"] = len(active_operators)
        
        return response_data
    except Exception as e:
        logger.error(f"Operator summary error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate operator summary")

@router.get("/api/get-models-stations", response_class=JSONResponse)
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

@router.get("/api/download_csv", response_class=StreamingResponse)
async def download_csv(
    date: str = None,
    week: str = None,
    month: str = None,
    start_date: str = None,
    end_date: str = None,
    active_filter: str = Query("all")
):
    """Download production data as CSV with 7 AM production day logic and active filter"""
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
        
        # Apply active filter if enabled
        if active_filter == "active":
            active_operators = get_active_operators(start_date, end_date)
            records = [r for r in records if r.operator in active_operators]

        # --- Prepare CSV ---
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Customer', 'Model', 'Station', 'Operator', 'Output', 'Cycle Time(s)',
                         'Target(s)', 'Start Time', 'End time', 'Status', 'Serial Numbers'])
        writer.writerow([f'# Production Day: Starts at 7:00 AM each day | Filter: {active_filter}'])

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
            'Content-Disposition': f'attachment; filename="production_data_{active_filter}.csv"'
        }

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers=headers
        )

    except Exception as e:
        logger.error(f"CSV download error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate CSV")

@router.get("/api/statistics", response_class=JSONResponse)
async def get_production_statistics(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    filters: Dict[str, Optional[str]] = Depends(get_filters)
):
    """Get comprehensive production statistics with 7 AM start logic and active filter"""
    try:
        records, date_str, _, _, _ = fetch_production_data(start_date, end_date)
        
        # Get active operators if filter is enabled
        active_operators = None
        if filters.get("active_filter") == "active":
            active_operators = get_active_operators(start_date, end_date)
        
        filtered_records = apply_filters(records, filters, active_operators)
        
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
        
        response_data = {
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
        
        if active_operators is not None:
            response_data["active_operators_count"] = len(active_operators)
        
        return response_data
    except Exception as e:
        logger.error(f"Statistics API error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate statistics")

@router.get("/api/production_day_info", response_class=JSONResponse)
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

@router.get("/health")
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