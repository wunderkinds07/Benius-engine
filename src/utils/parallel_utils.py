# parallel_utils.py - Parallel processing utilities

import os
import time
import concurrent.futures
from functools import partial

class ParallelProcessor:
    """Handles parallel processing of tasks using ThreadPoolExecutor or ProcessPoolExecutor"""
    
    def __init__(self, config):
        self.config = config
        self.parallel_enabled = config.get("parallel_processing", True)
        self.max_workers = config.get("max_workers", os.cpu_count() or 4)
        self.use_processes = config.get("use_processes", False)  # Default to threads
        self.chunk_size = config.get("chunk_size", 1)  # For map operations
    
    def process_items(self, items, task_function, *args, callback=None, **kwargs):
        """Process items in parallel using the specified task function
        
        Args:
            items: List of items to process
            task_function: Function to apply to each item
            args: Additional positional arguments for task_function
            callback: Optional callback function to call after each item is processed
            kwargs: Additional keyword arguments for task_function
            
        Returns:
            List of results from processing each item
        """
        if not self.parallel_enabled or len(items) <= 1:
            # Process sequentially if parallel is disabled or only one item
            results = []
            for item in items:
                result = task_function(item, *args, **kwargs)
                results.append(result)
                if callback:
                    callback(result)
            return results
        
        # Process in parallel
        executor_class = concurrent.futures.ProcessPoolExecutor if self.use_processes else concurrent.futures.ThreadPoolExecutor
        
        with executor_class(max_workers=self.max_workers) as executor:
            # If callback is provided, use submit for individual callbacks
            if callback:
                futures = [executor.submit(task_function, item, *args, **kwargs) for item in items]
                results = []
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    callback(result)
                    results.append(result)
                return results
            else:
                # Use map for simpler cases without callback
                if args or kwargs:
                    func = partial(task_function, *args, **kwargs)
                    return list(executor.map(func, items, chunksize=self.chunk_size))
                else:
                    return list(executor.map(task_function, items, chunksize=self.chunk_size))
    
    def process_batches(self, items, batch_size, task_function, *args, **kwargs):
        """Process items in batches rather than individually
        
        Args:
            items: List of items to process
            batch_size: Number of items per batch
            task_function: Function to apply to each batch
            args: Additional arguments for task_function
            kwargs: Additional keyword arguments for task_function
            
        Returns:
            List of results from processing each batch
        """
        # Create batches
        batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
        
        # Process batches (potentially in parallel)
        return self.process_items(batches, task_function, *args, **kwargs)
    
    def map_function(self, func, items, *args, **kwargs):
        """Map a function over items, potentially in parallel
        
        Args:
            func: Function to apply
            items: Iterable of items
            args: Additional arguments for func
            kwargs: Additional keyword arguments for func
            
        Returns:
            List of results
        """
        if not items:
            return []
            
        partial_func = partial(func, *args, **kwargs) if args or kwargs else func
        
        if not self.parallel_enabled or len(items) <= 1:
            return [partial_func(item) for item in items]
        
        executor_class = concurrent.futures.ProcessPoolExecutor if self.use_processes else concurrent.futures.ThreadPoolExecutor
        
        with executor_class(max_workers=self.max_workers) as executor:
            return list(executor.map(partial_func, items, chunksize=self.chunk_size))
    
    def run_in_parallel(self, functions_with_args):
        """Run multiple different functions in parallel
        
        Args:
            functions_with_args: List of (function, args, kwargs) tuples
            
        Returns:
            List of results in the same order as the input functions
        """
        if not self.parallel_enabled or len(functions_with_args) <= 1:
            # Run sequentially
            return [func(*args, **kwargs) for func, args, kwargs in functions_with_args]
        
        executor_class = concurrent.futures.ProcessPoolExecutor if self.use_processes else concurrent.futures.ThreadPoolExecutor
        
        with executor_class(max_workers=self.max_workers) as executor:
            futures = []
            for func, args, kwargs in functions_with_args:
                future = executor.submit(func, *args, **kwargs)
                futures.append(future)
                
            # Wait for all to complete and collect results
            return [future.result() for future in futures]

    def set_max_workers(self, workers):
        """Set the maximum number of workers
        
        Args:
            workers: Number of workers to use
            
        Returns:
            Self for method chaining
        """
        self.max_workers = max(1, int(workers))
        return self
    
    def enable_parallel(self, enabled=True):
        """Enable or disable parallel processing
        
        Args:
            enabled: Boolean indicating if parallel processing should be enabled
            
        Returns:
            Self for method chaining
        """
        self.parallel_enabled = enabled
        return self
    
    def use_process_pool(self, use_processes=True):
        """Set whether to use ProcessPoolExecutor instead of ThreadPoolExecutor
        
        Args:
            use_processes: Boolean indicating whether to use processes (True) or threads (False)
            
        Returns:
            Self for method chaining
        """
        self.use_processes = use_processes
        return self
