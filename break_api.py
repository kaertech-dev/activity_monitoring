"""API routes for break management"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime
from typing import Optional
import logging

from database import get_db_connection
from break_manager import BreakManager

logger = logging.getLogger(__name__)
router = APIRouter()


class BreakAction(BaseModel):
    """Model for break action requests"""
    operator_en: str
    action_type: str  # 'stop', 'play', 'pause', 'resume', 'break_start', 'break_end'
    station: Optional[str] = None
    notes: Optional[str] = None


@router.post("/api/break/action", response_class=JSONResponse)
async def record_break_action(action: BreakAction):
    """
    Record a break action (stop/play) for an operator
    
    Expected action_types:
    - 'stop', 'pause', 'break_start': Start of break
    - 'play', 'resume', 'break_end': End of break
    """
    try:
        with get_db_connection() as cursor:
            # Insert break action into database
            query = """
                INSERT INTO projectsdb.breaklogs 
                (operator_en, action_type, action_time, station, notes)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            action_time = datetime.now()
            
            cursor.execute(query, (
                action.operator_en,
                action.action_type,
                action_time,
                action.station,
                action.notes
            ))
            
            logger.info(f"Break action recorded: {action.operator_en} - {action.action_type}")
            
            return {
                "success": True,
                "operator": action.operator_en,
                "action": action.action_type,
                "timestamp": action_time.isoformat(),
                "message": f"Break action '{action.action_type}' recorded successfully"
            }
            
    except Exception as e:
        logger.error(f"Error recording break action: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to record break action: {str(e)}")


@router.get("/api/break/status/{operator_name}", response_class=JSONResponse)
async def get_break_status(operator_name: str):
    """Get current break status for an operator"""
    try:
        with get_db_connection() as cursor:
            # Get the most recent action for this operator
            query = """
                SELECT action_type, timestamp as action_time, station, notes
                FROM projectsdb.break_logs
                WHERE operator_en = %s
                ORDER BY action_time DESC
                LIMIT 1
            """
            
            cursor.execute(query, (operator_name,))
            result = cursor.fetchone()
            
            if not result:
                return {
                    "operator": operator_name,
                    "status": "active",
                    "last_action": None,
                    "message": "No break history found"
                }
            
            action_type, action_time, station, notes = result
            
            # Determine if operator is currently on break
            is_on_break = action_type.lower() in ['stop', 'pause', 'break_start']
            
            return {
                "operator": operator_name,
                "status": "on_break" if is_on_break else "active",
                "last_action": {
                    "type": action_type,
                    "time": action_time.isoformat(),
                    "station": station,
                    "notes": notes
                },
                "is_on_break": is_on_break
            }
            
    except Exception as e:
        logger.error(f"Error getting break status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get break status: {str(e)}")

@router.get("/api/break/history/{operator_name}", response_class=JSONResponse)
async def get_break_history(
    operator_name: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Get break history for an operator"""
    try:
        with get_db_connection() as cursor:
            from utils import get_production_start_time, get_production_date_range
            
            # Determine date range
            if start_date and end_date:
                start_dt, end_dt = get_production_date_range(start_date, end_date)
            else:
                start_dt, end_dt = get_production_start_time()
            
            # Fetch breaks for this operator
            breaks = BreakManager.get_operator_breaks(
                cursor, 
                operator_name, 
                start_dt, 
                end_dt
            )
            
            # Get break summary
            summary = BreakManager.get_break_summary(breaks)
            
            # Format breaks for response
            formatted_breaks = []
            for break_record in breaks:
                formatted_breaks.append({
                    "start": break_record['start'].isoformat() if break_record['start'] else None,
                    "end": break_record['end'].isoformat() if break_record['end'] else None,
                    "duration_minutes": round((break_record['end'] - break_record['start']).total_seconds() / 60, 2) 
                        if break_record['start'] and break_record['end'] else None
                })
            
            return {
                "operator": operator_name,
                "date_range": {
                    "start": start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    "end": end_dt.strftime('%Y-%m-%d %H:%M:%S')
                },
                "breaks": formatted_breaks,
                "summary": {
                    "total_breaks": summary['total_breaks'],
                    "total_break_time_minutes": round(summary['total_break_time'] / 60, 2),
                    "avg_break_duration_minutes": round(summary['avg_break_duration'] / 60, 2),
                    "longest_break_minutes": round(summary['longest_break'] / 60, 2)
                }
            }
            
    except Exception as e:
        logger.error(f"Error getting break history: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get break history: {str(e)}")


@router.get("/api/break/summary", response_class=JSONResponse)
async def get_all_breaks_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Get break summary for all operators"""
    try:
        with get_db_connection() as cursor:
            from utils import get_production_start_time, get_production_date_range
            
            # Determine date range
            if start_date and end_date:
                start_dt, end_dt = get_production_date_range(start_date, end_date)
            else:
                start_dt, end_dt = get_production_start_time()
            
            # Get all operators with break records
            query = """
                SELECT DISTINCT operator_en
                FROM projectsdb.breaklogs
                WHERE action_time BETWEEN %s AND %s
            """
            
            cursor.execute(query, (start_dt, end_dt))
            operators = [row[0] for row in cursor.fetchall()]
            
            operator_summaries = {}
            
            for operator in operators:
                breaks = BreakManager.get_operator_breaks(cursor, operator, start_dt, end_dt)
                summary = BreakManager.get_break_summary(breaks)
                
                operator_summaries[operator] = {
                    "total_breaks": summary['total_breaks'],
                    "total_break_time_minutes": round(summary['total_break_time'] / 60, 2),
                    "avg_break_duration_minutes": round(summary['avg_break_duration'] / 60, 2)
                }
            
            return {
                "date_range": {
                    "start": start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    "end": end_dt.strftime('%Y-%m-%d %H:%M:%S')
                },
                "operator_count": len(operators),
                "operators": operator_summaries,
                "production_day_info": "Break times are excluded from cycle time calculations"
            }
            
    except Exception as e:
        logger.error(f"Error getting breaks summary: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get breaks summary: {str(e)}")