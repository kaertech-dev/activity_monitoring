# activity_monitoring/api_routes.py
"""API route handlers - Refactored"""
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Optional
from datetime import datetime, timedelta
import logging
import csv
import io
import re

from services import fetch_production_data, apply_filters, get_active_operators
from utils import get_production_start_time
from database import get_db_connection
from config import db_config

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/operator_today", response_class=JSONResponse)
async def api_operator_today(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    customer: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    station: Optional[str] = Query(None),
    active_filter: Optional[str] = Query("all")
):
    """Get today's operator data"""
    try:
        records, date_str, _, _, _ = fetch_production_data(start_date, end_date)
        
        filters = {"customer": customer, "model": model, "station": station}
        active_operators = get_active_operators(start_date, end_date) if active_filter == "active" else None
        filtered_records = apply_filters(records, filters, active_operators)
        
        return {
            "date": date_str,
            "count": len(filtered_records),
            "records": [record.to_dict() for record in filtered_records],
            "active_operators_count": len(active_operators) if active_operators else None
        }
    except Exception as e:
        logger.error(f"API error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch data")


@router.get("/api/download_csv", response_class=StreamingResponse)
async def download_csv(
    date: Optional[str] = Query(None),
    week: Optional[str] = Query(None),
    month: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    active_filter: Optional[str] = Query("all")
):
    """Download production data as CSV"""
    try:
        # Parse date parameters
        if date:
            start_date = end_date = date
        elif week:
            match = re.match(r'^(\d{4})-W(\d{1,2})$', week)
            if match:
                year, week_num = int(match.group(1)), int(match.group(2))
                start_dt = datetime.fromisocalendar(year, week_num, 1)
                end_dt = datetime.fromisocalendar(year, week_num, 7)
                start_date = start_dt.strftime('%Y-%m-%d')
                end_date = end_dt.strftime('%Y-%m-%d')
        elif month:
            year, month_num = map(int, month.split('-'))
            start_dt = datetime(year, month_num, 1)
            end_dt = (datetime(year + 1, 1, 1) if month_num == 12 
                     else datetime(year, month_num + 1, 1)) - timedelta(days=1)
            start_date = start_dt.strftime('%Y-%m-%d')
            end_date = end_dt.strftime('%Y-%m-%d')

        # Fetch and filter records
        records, _, _, _, _ = fetch_production_data(start_date, end_date)
        
        if active_filter == "active":
            active_operators = get_active_operators(start_date, end_date)
            records = [r for r in records if r.operator in active_operators]

        # Generate CSV
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Customer', 'Model', 'Station', 'Operator', 'Output', 'Cycle Time(s)',
                         'Target(s)', 'Start Time', 'End time', 'Status', 'Serial Numbers'])

        for record in records:
            writer.writerow([
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
            ])

        output.seek(0)
        filename = f"production_data_{start_date or 'today'}_{active_filter}.csv"
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        logger.error(f"CSV error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/statistics", response_class=JSONResponse)
async def get_production_statistics(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    customer: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    station: Optional[str] = Query(None),
    active_filter: Optional[str] = Query("all")
):
    """Get production statistics"""
    try:
        records, date_str, _, _, _ = fetch_production_data(start_date, end_date)
        
        filters = {"customer": customer, "model": model, "station": station}
        active_operators = get_active_operators(start_date, end_date) if active_filter == "active" else None
        filtered_records = apply_filters(records, filters, active_operators)
        
        if not filtered_records:
            return {"message": "No data found"}
        
        # Calculate statistics
        total_output = sum(r.output for r in filtered_records)
        cycle_times = [r.cycle_time for r in filtered_records if r.cycle_time > 0]
        
        status_counts = {}
        operator_outputs = {}
        for record in filtered_records:
            status_counts[record.status] = status_counts.get(record.status, 0) + 1
            operator_outputs[record.operator] = operator_outputs.get(record.operator, 0) + record.output
        
        return {
            "summary": {
                "total_output": total_output,
                "total_operators": len(set(r.operator for r in filtered_records)),
                "avg_cycle_time": round(sum(cycle_times) / len(cycle_times), 2) if cycle_times else 0,
                "date_range": date_str
            },
            "status_distribution": status_counts,
            "top_operators": [{"operator": op, "output": out} 
                            for op, out in sorted(operator_outputs.items(), key=lambda x: x[1], reverse=True)[:5]],
            "active_operators_count": len(active_operators) if active_operators else None
        }
    except Exception as e:
        logger.error(f"Statistics error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate statistics")


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        with get_db_connection() as cursor:
            cursor.execute("SELECT 1")
            start_dt, end_dt = get_production_start_time()
            
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "database": "connected",
                "production_day": {
                    "start": start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    "end": end_dt.strftime('%Y-%m-%d %H:%M:%S')
                }
            }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")