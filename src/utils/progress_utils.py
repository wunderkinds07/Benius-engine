# progress_utils.py - Progress tracking utilities

import time
import sys

class ProgressManager:
    """Manages progress tracking and reporting for batch operations"""
    
    def __init__(self, config):
        self.config = config
        self.show_progress = config.get("show_progress", True)
        self.start_time = None
        self.total_items = 0
        self.processed_items = 0
        self.current_stage = ""
    
    def start(self, total_items, stage_name="Processing"):
        """Start progress tracking for a batch operation
        
        Args:
            total_items: Total number of items to process
            stage_name: Name of the current processing stage
            
        Returns:
            Boolean indicating if progress tracking was started
        """
        self.start_time = time.time()
        self.total_items = total_items
        self.processed_items = 0
        self.current_stage = stage_name
        
        if self.show_progress:
            print(f"\n{stage_name} started. Total items: {total_items}")
            
        return True
    
    def update(self, increment=1, **stats):
        """Update the progress counter
        
        Args:
            increment: Number of items completed in this update
            stats: Optional statistics to display
            
        Returns:
            Percentage of completion
        """
        self.processed_items += increment
        percentage = (self.processed_items / self.total_items) * 100 if self.total_items > 0 else 0
        
        if self.show_progress:
            # Create progress bar
            bar_length = 30
            filled_length = int(bar_length * self.processed_items // self.total_items)
            bar = '█' * filled_length + '░' * (bar_length - filled_length)
            
            # Calculate ETA
            elapsed = time.time() - self.start_time
            items_per_sec = self.processed_items / elapsed if elapsed > 0 else 0
            remaining_items = self.total_items - self.processed_items
            eta = remaining_items / items_per_sec if items_per_sec > 0 else 0
            
            # Format stats string
            stats_str = ""
            if stats:
                stats_str = ", ".join([f"{k}: {v}" for k, v in stats.items()])
                if stats_str:
                    stats_str = f" | {stats_str}"
            
            # Print progress
            sys.stdout.write(f"\r{self.current_stage}: [{bar}] {percentage:.1f}% ({self.processed_items}/{self.total_items}) "
                             f"| ETA: {eta:.1f}s{stats_str}")
            sys.stdout.flush()
            
        return percentage
    
    def finish(self):
        """Complete the progress tracking and show summary
        
        Returns:
            Dictionary with summary statistics
        """
        elapsed = time.time() - self.start_time if self.start_time else 0
        summary = {
            "stage": self.current_stage,
            "total_items": self.total_items,
            "processed_items": self.processed_items,
            "elapsed_seconds": elapsed,
            "items_per_second": self.processed_items / elapsed if elapsed > 0 else 0
        }
        
        if self.show_progress:
            print(f"\n{self.current_stage} completed in {elapsed:.2f} seconds. "
                  f"Processed {self.processed_items}/{self.total_items} items "
                  f"({summary['items_per_second']:.2f} items/sec)")
            
        return summary
