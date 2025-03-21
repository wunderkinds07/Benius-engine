# memory_utils.py - Memory optimization utilities

import os
import gc
import psutil
import logging
from functools import wraps

class MemoryOptimizer:
    """Provides utilities for optimizing memory usage during batch processing"""
    
    def __init__(self, config=None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        self.memory_threshold = self.config.get("memory_threshold", 80)  # Percent
        self.low_memory_threshold = self.config.get("low_memory_threshold", 95)  # Percent
        self.batch_size = self.config.get("memory_batch_size", 100)  # Files per batch
        self.process = psutil.Process(os.getpid())
    
    def get_memory_usage(self):
        """Get current memory usage percentage
        
        Returns:
            Dictionary with memory usage statistics
        """
        try:
            # Get process memory info
            process_memory = self.process.memory_info().rss / (1024 * 1024)  # MB
            
            # Get system memory info
            system_memory = psutil.virtual_memory()
            system_used_percent = system_memory.percent
            system_available = system_memory.available / (1024 * 1024)  # MB
            system_total = system_memory.total / (1024 * 1024)  # MB
            
            return {
                "process_memory_mb": process_memory,
                "system_memory_percent": system_used_percent,
                "system_available_mb": system_available,
                "system_total_mb": system_total,
                "is_low_memory": system_used_percent > self.memory_threshold,
                "is_critical_memory": system_used_percent > self.low_memory_threshold
            }
        except Exception as e:
            self.logger.warning(f"Error getting memory usage: {e}")
            return {
                "process_memory_mb": 0,
                "system_memory_percent": 0,
                "system_available_mb": 0,
                "system_total_mb": 0,
                "is_low_memory": False,
                "is_critical_memory": False
            }
    
    def optimize_memory(self, force=False):
        """Attempt to free up memory
        
        Args:
            force: Force garbage collection regardless of threshold
            
        Returns:
            Dictionary with before/after memory usage
        """
        before = self.get_memory_usage()
        
        # Only optimize if we're above threshold or forced
        if force or before["is_low_memory"]:
            # Force garbage collection
            gc.collect()
            
            # If critical memory situation, take more aggressive measures
            if before["is_critical_memory"]:
                # Clear any module-level caches here if needed
                pass
                
        after = self.get_memory_usage()
        
        # Log memory optimization results
        if force or before["is_low_memory"]:
            freed_mb = before["process_memory_mb"] - after["process_memory_mb"]
            self.logger.info(f"Memory optimization freed {freed_mb:.2f} MB")
            
        return {"before": before, "after": after}
    
    def batch_generator(self, items, batch_size=None):
        """Split a large list into memory-efficient batches
        
        Args:
            items: List of items to process
            batch_size: Optional custom batch size
            
        Yields:
            Batches of items
        """
        batch_size = batch_size or self.batch_size
        total_items = len(items)
        
        self.logger.info(f"Processing {total_items} items in batches of {batch_size}")
        
        # Process in batches
        for i in range(0, total_items, batch_size):
            # Check memory before yielding batch
            memory_info = self.get_memory_usage()
            
            # If memory is critical, reduce batch size for next iteration
            if memory_info["is_critical_memory"]:
                new_batch_size = max(10, batch_size // 2)
                self.logger.warning(
                    f"Low memory detected ({memory_info['system_memory_percent']:.1f}%). "
                    f"Reducing batch size from {batch_size} to {new_batch_size}."
                )
                batch_size = new_batch_size
                
                # Force memory optimization
                self.optimize_memory(force=True)
            
            # Yield the current batch
            end_idx = min(i + batch_size, total_items)
            self.logger.debug(f"Yielding batch {i//batch_size + 1}: items {i} to {end_idx-1}")
            yield items[i:end_idx]
            
            # After processing a batch, optimize memory
            if i + batch_size < total_items:  # Not the last batch
                self.optimize_memory()


def memory_efficient(func=None, batch_size=None):
    """Decorator to make functions memory-efficient with batch processing
    
    Args:
        func: Function to decorate
        batch_size: Optional custom batch size
        
    Returns:
        Decorated function
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            # Extract list/dict argument which will be batched
            # Assuming the first argument that's a list or dict is what we want to batch
            batch_arg = None
            batch_arg_index = None
            
            # Look through args for a list or dict
            for i, arg in enumerate(args):
                if isinstance(arg, (list, dict)):
                    batch_arg = arg
                    batch_arg_index = i
                    break
            
            # If no batch arg found in args, look in kwargs
            if batch_arg is None:
                for key, value in kwargs.items():
                    if isinstance(value, (list, dict)):
                        batch_arg = value
                        batch_arg_index = key
                        break
            
            # If nothing to batch, just call the function
            if batch_arg is None:
                return f(*args, **kwargs)
            
            # Create optimizer with the config from the object if available
            if hasattr(args[0], 'config'):
                optimizer = MemoryOptimizer(args[0].config)
            else:
                optimizer = MemoryOptimizer()
            
            # Use the specified batch size or optimizer's default
            actual_batch_size = batch_size or optimizer.batch_size
            
            results = []
            is_dict = isinstance(batch_arg, dict)
            
            # Convert dict to list for batching if needed
            if is_dict:
                items = list(batch_arg.items())
            else:
                items = batch_arg
            
            # Process in batches
            for batch in optimizer.batch_generator(items, batch_size=actual_batch_size):
                # Reconstruct the batch argument
                if is_dict:
                    batch_dict = dict(batch)
                    if batch_arg_index is not None and isinstance(batch_arg_index, int):
                        # Replace in args
                        new_args = list(args)
                        new_args[batch_arg_index] = batch_dict
                        batch_result = f(*new_args, **kwargs)
                    else:
                        # Replace in kwargs
                        new_kwargs = kwargs.copy()
                        new_kwargs[batch_arg_index] = batch_dict
                        batch_result = f(*args, **new_kwargs)
                else:
                    if batch_arg_index is not None and isinstance(batch_arg_index, int):
                        # Replace in args
                        new_args = list(args)
                        new_args[batch_arg_index] = batch
                        batch_result = f(*new_args, **kwargs)
                    else:
                        # Replace in kwargs
                        new_kwargs = kwargs.copy()
                        new_kwargs[batch_arg_index] = batch
                        batch_result = f(*args, **new_kwargs)
                
                # Collect results
                if batch_result is not None:
                    if isinstance(batch_result, dict) and is_dict:
                        # Merge dict results
                        results.append(batch_result)
                    elif isinstance(batch_result, list):
                        # Extend list results
                        results.extend(batch_result)
                    else:
                        # Append any other result type
                        results.append(batch_result)
            
            # Combine results appropriately
            if not results:
                return None
            elif isinstance(results[0], dict):
                # Merge dictionaries
                combined = {}
                for r in results:
                    combined.update(r)
                return combined
            else:
                # Return list of results
                return results
                
        return wrapper
        
    if func is None:
        return decorator
    return decorator(func)
