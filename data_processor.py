"""Production data processing and statistics"""
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class ProductionDataProcessor:
    """Enhanced production data processing with statistical analysis"""
    @staticmethod
    def get_table_columns(cursor, database: str, table: str) -> List[tuple]:
        """Get column information for a table"""
         # Validate database and table names to prevent SQL injection  
        if not database.replace('_', '').replace('-', '').isalnum():  
            raise ValueError("Invalid database name")  
        if not table.replace('_', '').replace('-', '').isalnum():  
            raise ValueError("Invalid table name")  
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
        """Calculate various cycle time statistics with 8-hour shift adjustment"""
        if len(timestamps) < 2:
            return {"avg_cycle_time": 0, "durations": []}
        
        # Add 8 hours to all timestamps to shift from Shift B to Shift A equivalent
        adjusted_timestamps = [ts + timedelta(hours=7.75) for ts in timestamps]
        
        # Calculate durations between consecutive records using adjusted timestamps
        durations = []
        for i in range(1, len(adjusted_timestamps)):
            diff = (adjusted_timestamps[i] - adjusted_timestamps[i-1]).total_seconds()
            if diff > 0:
                durations.append(diff)
        
        if not durations:
            total_duration = (adjusted_timestamps[-1] - adjusted_timestamps[0]).total_seconds()
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
            "total_duration": (adjusted_timestamps[-1] - adjusted_timestamps[0]).total_seconds()
        }