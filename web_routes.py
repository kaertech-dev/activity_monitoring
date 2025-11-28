# activity_monitoring/web_routes.py
"""Web page route handlers - Refactored"""
from fastapi import APIRouter, Request, Query, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from datetime import datetime, timedelta
import logging
import re

from services import fetch_production_data, apply_filters, get_operator_statistics, get_active_operators
from utils import get_production_start_time
import os

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="templates")


def parse_date_parameters(
    date: Optional[str] = None,
    week: Optional[str] = None,
    month: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> tuple[Optional[str], Optional[str]]:
    """Parse various date formats and return (start_date, end_date) tuple"""
    today = datetime.now().date().strftime('%Y-%m-%d')
    
    # Direct date range
    if start_date and end_date:
        if start_date == today and end_date == today:
            return None, None
        return start_date, end_date
    
    # Single date
    if date:
        return (None, None) if date == today else (date, date)
    
    # Week selection
    if week:
        try:
            match = re.match(r'^(\d{4})-W(\d{1,2})$', week)
            if match:
                year, week_num = int(match.group(1)), int(match.group(2))
                start_dt = datetime.fromisocalendar(year, week_num, 1)
                end_dt = datetime.fromisocalendar(year, week_num, 7)
                return start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')
        except Exception as e:
            logger.error(f"Error parsing week '{week}': {e}")
    
    # Month selection
    if month:
        try:
            year, month_num = map(int, month.split('-'))
            start_dt = datetime(year, month_num, 1)
            end_dt = (datetime(year + (month_num // 12), (month_num % 12) + 1, 1) - timedelta(days=1) 
                     if month_num < 12 else datetime(year + 1, 1, 1) - timedelta(days=1))
            return start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')
        except Exception as e:
            logger.error(f"Error parsing month '{month}': {e}")
    
    return None, None


@router.get("/", response_class=HTMLResponse)
async def show_operator_en_today(
    request: Request,
    date: Optional[str] = Query(None),
    week: Optional[str] = Query(None),
    month: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    customer: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    station: Optional[str] = Query(None),
    active_filter: Optional[str] = Query("all")
):
    """Main dashboard endpoint"""
    try:
        # Parse dates
        parsed_start, parsed_end = parse_date_parameters(date, week, month, start_date, end_date)
        
        # Fetch data
        records, _, active_databases, models, stations = fetch_production_data(parsed_start, parsed_end)
        
        # Apply filters
        filters = {"customer": customer, "model": model, "station": station}
        active_operators = get_active_operators(parsed_start, parsed_end) if active_filter == "active" else None
        filtered_records = apply_filters(records, filters, active_operators)
        
        # Prepare table data
        columns = ['Customer', 'Model', 'Station', 'Operator', 'Output', 
                   'Cycle Time(s)', 'Target(s)', 'Start Time', 'End time', 'Status']
        rows = [tuple(record.to_dict()[col] for col in columns) for record in filtered_records]
        
        # Date display
        if parsed_start and parsed_end:
            current_date_display = parsed_start if parsed_start == parsed_end else f"{parsed_start} → {parsed_end}"
        else:
            start_dt, _ = get_production_start_time()
            current_date_display = start_dt.strftime('%Y-%m-%d')
        
        return templates.TemplateResponse("monitoring_v1.html", {
            "request": request,
            "rows": rows,
            "columns": columns,
            "current_date": current_date_display,
            "start_date": parsed_start or start_date,
            "end_date": parsed_end or end_date,
            "selected_db": customer,
            "selected_model": model,
            "selected_station": station,
            "active_filter": active_filter,
            "db": active_databases,
            "models": models,
            "stations": stations,
            "total_records": len(filtered_records),
            "active_operators_count": len(active_operators) if active_operators else None
        })
    except Exception as e:
        logger.error(f"Dashboard error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/operator/{operator_name}", response_class=HTMLResponse)
async def show_operator_activity(request: Request, operator_name: str):
    """Show specific operator activity"""
    try:
        records, today_str, _, _, _ = fetch_production_data()
        stats = get_operator_statistics(records, operator_name)
        
        columns = ['Customer', 'Model', 'Station', 'Operator', 'Output',
                   'Cycle Time(s)', 'Target(s)', 'Start Time', 'End time', 'Status']
        rows = [tuple(record.to_dict()[col] for col in columns) for record in stats['records']]
        
        return templates.TemplateResponse("operator_activity.html", {
            "request": request,
            "rows": rows,
            "columns": columns,
            "current_date": today_str,
            "operator_name": operator_name,
            "serials": stats['serials'],
            "operator_stats": {
                "total_output": stats['total_output'],
                "total_duration_hours": stats['total_duration_hours'],
                "mode_cycle_time": stats['mode_cycle_time'],
                "stations_count": stats['stations_count']
            }
        })
    except Exception as e:
        logger.error(f"Operator activity error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch operator data")