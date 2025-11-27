# activity_monitoring/web_routes.py
"""Web page route handlers with Active Filter support"""
from fastapi import APIRouter, Request, Query, HTTPException, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import Optional, Dict
import logging

from services import fetch_production_data, apply_filters, get_operator_statistics, get_active_operators
from utils import get_production_start_time
import os


logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="templates")


# Dependency for common filtering
def get_filters(
    customer: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    station: Optional[str] = Query(None),
    active_filter: Optional[str] = Query("all")  # New parameter for active filter
):
    return {
        "customer": customer, 
        "model": model, 
        "station": station,
        "active_filter": active_filter
    }

@router.get("/", response_class=HTMLResponse)
async def show_operator_en_today(
    request: Request,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    filters: Dict[str, Optional[str]] = Depends(get_filters),
    submit_customer: Optional[str] = Query(None),
    submit_model: Optional[str] = Query(None),
    submit_station: Optional[str] = Query(None)
):
    """Main dashboard endpoint with 7 AM production day start and active filter"""
    try:
        records, date_str, active_databases, models, stations = fetch_production_data(start_date, end_date)
        
        # Get active operators if filter is set to "active"
        active_operators = None
        if filters.get("active_filter") == "active":
            active_operators = get_active_operators(start_date, end_date)
            logger.info(f"Active filter enabled: {len(active_operators)} active operators")
        
        # Apply all filters including active filter
        filtered_records = apply_filters(records, filters, active_operators)
        
        columns = ['Customer', 'Model', 'Station', 'Operator', 'Output', 'Cycle Time(s)', 'Target(s)', 'Start Time', 'End time', 'Status']
        rows = [tuple(record.to_dict()[col] for col in columns) for record in filtered_records]
        
        # Enhanced date display with production day info
        if start_date and end_date:
            current_date_display = f"{start_date} → {end_date}"
        else:
            start_dt, end_dt = get_production_start_time()
            current_date_display = f"{start_dt.strftime('%Y-%m-%d')}"
        
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
            "active_filter": filters.get("active_filter", "all"),  # Pass active filter to template
            "db": active_databases,
            "models": models,
            "stations": stations,
            "total_records": len(filtered_records),
            "active_operators_count": len(active_operators) if active_operators else None
        })
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/operator/{operator_name}", response_class=HTMLResponse)
async def show_operator_activity(request: Request, operator_name: str):
    """Show specific operator activity with enhanced details and 7 AM start"""
    try:
        records, today_str, _, _, _ = fetch_production_data()
        stats = get_operator_statistics(records, operator_name)
        
        columns = ['Customer', 'Model', 'Station', 'Operator', 'Output',
                   'Cycle Time(s)', 'Target(s)', 'Start Time', 'End time', 'Status']
        rows = [tuple(record.to_dict()[col] for col in columns) for record in stats['records']]
        
        operator_stats = {
            "total_output": stats['total_output'],
            "total_duration_hours": stats['total_duration_hours'],
            "mode_cycle_time": stats['mode_cycle_time'],
            "stations_count": stats['stations_count']
        }
        
        return templates.TemplateResponse("operator_activity.html", {
            "request": request,
            "rows": rows,
            "columns": columns,
            "current_date": today_str,
            "operator_name": operator_name,
            "serials": stats['serials'],
            "operator_stats": operator_stats
        })
    except Exception as e:
        logger.error(f"Operator activity error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch operator data")
logger.info(f"Template directory: {os.path.abspath('templates')}")