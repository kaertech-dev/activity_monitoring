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