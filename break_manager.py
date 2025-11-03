"""Break time management and tracking"""
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class BreakManager:
    """Manage operator break times and calculate adjusted cycle times"""
    
    @staticmethod
    def get_operator_breaks(
        cursor, 
        operator: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[Dict]:
        """
        Fetch break records for an operator within a time range
        
        Args:
            cursor: Database cursor
            operator: Operator name
            start_time: Start of production period
            end_time: End of production period
            
        Returns:
            List of break records with start and end times
        """
        try:
            query = """
                SELECT 
                    action_type,
                    action_time,
                    operator_en
                FROM projectsdb.breaklogs
                WHERE operator_en = %s 
                AND action_time BETWEEN %s AND %s
                ORDER BY action_time
            """
            cursor.execute(query, (operator, start_time, end_time))
            results = cursor.fetchall()
            
            breaks = []
            current_break = None
            
            for action_type, action_time, _ in results:
                if action_type.lower() in ['stop', 'pause', 'break_start']:
                    # Start of a break
                    current_break = {
                        'start': action_time,
                        'end': None
                    }
                elif action_type.lower() in ['play', 'resume', 'break_end']:
                    # End of a break
                    if current_break and current_break['end'] is None:
                        current_break['end'] = action_time
                        breaks.append(current_break)
                        current_break = None
            
            # If there's an unclosed break, close it at end_time
            if current_break and current_break['end'] is None:
                current_break['end'] = end_time
                breaks.append(current_break)
            
            logger.info(f"Found {len(breaks)} break periods for operator {operator}")
            return breaks
            
        except Exception as e:
            logger.error(f"Error fetching breaks for operator {operator}: {e}")
            return []
    
    @staticmethod
    def calculate_total_break_time(breaks: List[Dict]) -> float:
        """
        Calculate total break time in seconds
        
        Args:
            breaks: List of break records with 'start' and 'end' keys
            
        Returns:
            Total break time in seconds
        """
        total_seconds = 0
        for break_record in breaks:
            if break_record['start'] and break_record['end']:
                duration = (break_record['end'] - break_record['start']).total_seconds()
                total_seconds += duration
        
        return total_seconds
    
    @staticmethod
    def adjust_cycle_time_for_breaks(
        timestamps: List[datetime],
        breaks: List[Dict],
        output_count: int
    ) -> Tuple[float, float, List[float]]:
        """
        Calculate cycle time excluding break periods
        
        Args:
            timestamps: List of production timestamps
            breaks: List of break periods
            output_count: Number of units produced
            
        Returns:
            Tuple of (adjusted_cycle_time, total_break_time, adjusted_durations)
        """
        if len(timestamps) < 2:
            return 0, 0, []
        
        # Calculate durations between consecutive records
        durations = []
        adjusted_durations = []
        
        for i in range(1, len(timestamps)):
            start = timestamps[i-1]
            end = timestamps[i]
            duration = (end - start).total_seconds()
            
            if duration > 0:
                # Calculate break time within this production interval
                break_time_in_interval = 0
                for break_record in breaks:
                    if not break_record['start'] or not break_record['end']:
                        continue
                    
                    break_start = break_record['start']
                    break_end = break_record['end']
                    
                    # Check if break overlaps with this production interval
                    if break_start < end and break_end > start:
                        # Calculate overlap
                        overlap_start = max(start, break_start)
                        overlap_end = min(end, break_end)
                        overlap_duration = (overlap_end - overlap_start).total_seconds()
                        break_time_in_interval += max(0, overlap_duration)
                
                # Adjusted duration = actual duration - break time
                adjusted_duration = duration - break_time_in_interval
                durations.append(duration)
                
                if adjusted_duration > 0:
                    adjusted_durations.append(adjusted_duration)
        
        if not adjusted_durations:
            # No valid durations, calculate based on total time
            total_time = (timestamps[-1] - timestamps[0]).total_seconds()
            total_break_time = BreakManager.calculate_total_break_time(breaks)
            adjusted_total_time = total_time - total_break_time
            avg_cycle_time = adjusted_total_time / output_count if output_count > 0 else 0
            return max(0, avg_cycle_time), total_break_time, []
        
        # Calculate average of 3 shortest adjusted durations
        sorted_adjusted = sorted(adjusted_durations)
        if len(sorted_adjusted) >= 3:
            avg_cycle_time = sum(sorted_adjusted[:3]) / 3
        else:
            avg_cycle_time = sum(sorted_adjusted) / len(sorted_adjusted)
        
        total_break_time = BreakManager.calculate_total_break_time(breaks)
        
        return avg_cycle_time, total_break_time, adjusted_durations
    
    @staticmethod
    def get_break_summary(breaks: List[Dict]) -> Dict:
        """
        Generate a summary of break activities
        
        Args:
            breaks: List of break records
            
        Returns:
            Dictionary with break statistics
        """
        if not breaks:
            return {
                'total_breaks': 0,
                'total_break_time': 0,
                'avg_break_duration': 0,
                'longest_break': 0
            }
        
        total_time = 0
        durations = []
        
        for break_record in breaks:
            if break_record['start'] and break_record['end']:
                duration = (break_record['end'] - break_record['start']).total_seconds()
                durations.append(duration)
                total_time += duration
        
        return {
            'total_breaks': len(breaks),
            'total_break_time': round(total_time, 2),
            'avg_break_duration': round(total_time / len(breaks), 2) if breaks else 0,
            'longest_break': round(max(durations), 2) if durations else 0
        }