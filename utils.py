# activity_monitoring/utils.py
"""Utility functions for Production Monitoring System"""
from datetime import datetime, timedelta
from typing import Tuple


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