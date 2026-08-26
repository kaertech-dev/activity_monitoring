"""Data models for Production Monitoring System"""
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime

@dataclass
class ProductionRecord:
    """Data class for production records with enhanced statistics"""
    customer: str
    model: str
    station: str
    operator: str  # This will now be the employee name
    operator_code: str  # This stores the original operator_en code (e.g., 'KE0447')
    output: int
    target_time: Optional[float]
    cycle_time: float
    start_time: datetime
    end_time: datetime
    status: str
    serial_nums: List[str]
    duration_hours: Optional[float] = None
    individual_durations: List[float] = None
    # NEW: common/display name for the station, resolved from projectsdb.station.cname.
    # Falls back to `station` when no cname mapping exists. `station` itself is left
    # untouched so lookups (target_time, table naming, _main ordering) keep working.
    station_display: Optional[str] = None
    # NEW: 0-based position of this station within its model's _main sequence.
    # None when the station couldn't be placed (not found in _main / no _main table).
    station_order: Optional[int] = None

    def _display_station(self) -> str:
        return (self.station_display or self.station)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'Customer': self.customer.upper(),
            'Model': self.model.upper(),
            'Station': self._display_station().upper(),
            'Operator': self.operator,  # Now displays employee name
            'Output': self.output,
            'Target(s)': self.target_time,
            'Cycle Time(s)': f"{self.cycle_time:.2f}" if self.cycle_time != 0 else '-',
            'Start Time': self.start_time.strftime('%H:%M:%S') if self.start_time else None,
            'End time': self.end_time.strftime('%H:%M:%S') if self.end_time else None,
            'Status': self.status,
            'serial_num': self.serial_nums,
            'operator_code': self.operator_code,  # Include code for reference if needed
            'station_code': self.station,  # Raw station code, for reference/debugging
        }