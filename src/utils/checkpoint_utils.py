# checkpoint_utils.py - Checkpoint management utilities

import os
import json
import pickle
import time
from pathlib import Path

class CheckpointManager:
    """Manages checkpoints for state persistence during image processing"""
    
    def __init__(self, config):
        self.config = config
        self.enable_checkpoints = config.get("enable_checkpoints", True)
        self.checkpoint_dir = config.get("checkpoint_directory", "checkpoints")
        self.checkpoint_interval = config.get("checkpoint_interval_seconds", 60)  # Default: every minute
        self.last_checkpoint_time = 0
        
        # Ensure checkpoint directory exists
        if self.enable_checkpoints:
            os.makedirs(self.checkpoint_dir, exist_ok=True)
    
    def should_create_checkpoint(self):
        """Check if it's time to create a new checkpoint based on the interval
        
        Returns:
            Boolean indicating if checkpoint should be created
        """
        if not self.enable_checkpoints:
            return False
            
        current_time = time.time()
        time_since_last = current_time - self.last_checkpoint_time
        
        return time_since_last >= self.checkpoint_interval
    
    def save_checkpoint(self, batch_id, state_data):
        """Save processing state to a checkpoint file
        
        Args:
            batch_id: Identifier for the batch
            state_data: Dictionary containing state to save
            
        Returns:
            Path to the created checkpoint file or None if checkpoints disabled
        """
        if not self.enable_checkpoints:
            return None
            
        try:
            # Create checkpoint filename
            timestamp = int(time.time())
            checkpoint_filename = f"{batch_id}_{timestamp}.checkpoint"
            checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_filename)
            
            # Add metadata to state data
            state_data['_metadata'] = {
                'batch_id': batch_id,
                'timestamp': timestamp,
                'checkpoint_id': checkpoint_filename
            }
            
            # Save state
            with open(checkpoint_path, 'wb') as f:
                pickle.dump(state_data, f)
                
            # Also save a JSON index file for human readability
            index_path = os.path.join(self.checkpoint_dir, f"{batch_id}_index.json")
            
            # Load existing index if available
            index_data = {}
            if os.path.exists(index_path):
                with open(index_path, 'r') as f:
                    try:
                        index_data = json.load(f)
                    except json.JSONDecodeError:
                        index_data = {}
            
            # Update index
            checkpoint_info = {
                'filename': checkpoint_filename,
                'timestamp': timestamp,
                'datetime': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp)),
                'state_keys': list(state_data.keys())
            }
            
            if 'checkpoints' not in index_data:
                index_data['checkpoints'] = []
                
            index_data['checkpoints'].append(checkpoint_info)
            
            # Save updated index
            with open(index_path, 'w') as f:
                json.dump(index_data, f, indent=2)
            
            # Update last checkpoint time
            self.last_checkpoint_time = time.time()
            
            return checkpoint_path
            
        except Exception as e:
            print(f"Error saving checkpoint: {e}")
            return None
    
    def load_checkpoint(self, checkpoint_path):
        """Load processing state from a checkpoint file
        
        Args:
            checkpoint_path: Path to the checkpoint file
            
        Returns:
            Dictionary containing the loaded state or None if loading failed
        """
        if not os.path.exists(checkpoint_path):
            return None
            
        try:
            with open(checkpoint_path, 'rb') as f:
                state_data = pickle.load(f)
            return state_data
        except Exception as e:
            print(f"Error loading checkpoint: {e}")
            return None
    
    def get_latest_checkpoint(self, batch_id):
        """Get the latest checkpoint for a specific batch
        
        Args:
            batch_id: Identifier for the batch
            
        Returns:
            Path to the latest checkpoint file or None if not found
        """
        if not self.enable_checkpoints:
            return None
            
        # Look for index file first
        index_path = os.path.join(self.checkpoint_dir, f"{batch_id}_index.json")
        if os.path.exists(index_path):
            try:
                with open(index_path, 'r') as f:
                    index_data = json.load(f)
                    
                if 'checkpoints' in index_data and index_data['checkpoints']:
                    # Sort checkpoints by timestamp and get the latest
                    checkpoints = sorted(index_data['checkpoints'], key=lambda x: x['timestamp'], reverse=True)
                    latest = checkpoints[0]['filename']
                    latest_path = os.path.join(self.checkpoint_dir, latest)
                    
                    if os.path.exists(latest_path):
                        return latest_path
            except Exception as e:
                print(f"Error reading checkpoint index: {e}")
        
        # Fallback: find checkpoints by filename pattern
        pattern = f"{batch_id}_*.checkpoint"
        checkpoint_files = list(Path(self.checkpoint_dir).glob(pattern))
        
        if not checkpoint_files:
            return None
            
        # Sort by modification time and return the latest
        latest = max(checkpoint_files, key=lambda p: p.stat().st_mtime)
        return str(latest)
    
    def list_checkpoints(self, batch_id=None):
        """List available checkpoints
        
        Args:
            batch_id: Optional batch identifier to filter checkpoints
            
        Returns:
            List of checkpoint information dictionaries
        """
        if not self.enable_checkpoints or not os.path.exists(self.checkpoint_dir):
            return []
            
        checkpoints = []
        
        # If batch_id provided, look for its index file
        if batch_id:
            index_path = os.path.join(self.checkpoint_dir, f"{batch_id}_index.json")
            if os.path.exists(index_path):
                try:
                    with open(index_path, 'r') as f:
                        index_data = json.load(f)
                    if 'checkpoints' in index_data:
                        return index_data['checkpoints']
                except Exception as e:
                    print(f"Error reading checkpoint index: {e}")
        
        # Fallback or when no batch_id is provided: scan the directory
        pattern = "*.checkpoint" if batch_id is None else f"{batch_id}_*.checkpoint"
        checkpoint_files = list(Path(self.checkpoint_dir).glob(pattern))
        
        for cp_file in checkpoint_files:
            try:
                # Extract batch_id and timestamp from filename
                filename = cp_file.name
                parts = filename.split('_')
                if len(parts) >= 2:
                    batch_id = parts[0]
                    timestamp_str = parts[1].split('.')[0]
                    timestamp = int(timestamp_str)
                    
                    checkpoints.append({
                        'filename': str(cp_file.name),
                        'path': str(cp_file),
                        'batch_id': batch_id,
                        'timestamp': timestamp,
                        'datetime': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp)),
                        'size': cp_file.stat().st_size
                    })
            except Exception as e:
                print(f"Error parsing checkpoint file {cp_file}: {e}")
        
        # Sort by timestamp (descending)
        checkpoints.sort(key=lambda x: x['timestamp'], reverse=True)
        return checkpoints
    
    def cleanup_old_checkpoints(self, batch_id, keep_count=5):
        """Remove old checkpoints keeping only the latest few
        
        Args:
            batch_id: Batch identifier to clean up
            keep_count: Number of recent checkpoints to keep
            
        Returns:
            Number of checkpoints removed
        """
        if not self.enable_checkpoints:
            return 0
            
        # Get list of checkpoints for this batch
        checkpoints = self.list_checkpoints(batch_id)
        
        if len(checkpoints) <= keep_count:
            return 0
            
        # Keep the latest 'keep_count' checkpoints
        checkpoints_to_delete = checkpoints[keep_count:]
        deleted_count = 0
        
        for cp in checkpoints_to_delete:
            try:
                cp_path = os.path.join(self.checkpoint_dir, cp['filename'])
                if os.path.exists(cp_path):
                    os.remove(cp_path)
                    deleted_count += 1
            except Exception as e:
                print(f"Error deleting checkpoint {cp['filename']}: {e}")
        
        # Update index if it exists
        index_path = os.path.join(self.checkpoint_dir, f"{batch_id}_index.json")
        if os.path.exists(index_path):
            try:
                with open(index_path, 'r') as f:
                    index_data = json.load(f)
                
                if 'checkpoints' in index_data:
                    # Keep only the latest checkpoints in the index
                    index_data['checkpoints'] = index_data['checkpoints'][:keep_count]
                    
                    with open(index_path, 'w') as f:
                        json.dump(index_data, f, indent=2)
            except Exception as e:
                print(f"Error updating checkpoint index: {e}")
        
        return deleted_count
