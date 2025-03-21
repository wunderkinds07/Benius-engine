# renamer.py - Image renaming module

import os
import shutil
from pathlib import Path

class Renamer:
    """Handles renaming of images according to batch ID convention"""
    
    def __init__(self, config):
        self.config = config
        self.batch_prefix = config.get("batch_prefix", "bid")  # Should be "bid" per requirements
    
    def rename_file(self, file_path, output_dir, batch_id, sequence_number):
        """Rename a file using the batch ID convention
        
        Args:
            file_path: Path to the original file
            output_dir: Directory to save renamed file
            batch_id: Batch identifier
            sequence_number: Sequential number for this image
            
        Returns:
            Path to the renamed file
        """
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Get file extension
        file_extension = Path(file_path).suffix
        
        # Generate new filename using the 'bidXXXXXX' format as specified in the requirements
        # Format should be "bidXXXXXX" where XXXXXX is a 6-digit sequence number
        new_filename = f"{self.batch_prefix}{sequence_number:06d}{file_extension}"
        new_path = os.path.join(output_dir, new_filename)
        
        try:
            # Copy file with new name
            shutil.copy2(file_path, new_path)
            return new_path
        except Exception as e:
            print(f"Error renaming {file_path}: {e}")
            return None
    
    def process(self, file_paths, output_dir, batch_id=None):
        """Process a list of files and rename them
        
        Args:
            file_paths: List of file paths to process
            output_dir: Directory to save renamed files
            batch_id: Optional batch identifier
            
        Returns:
            Dictionary mapping original paths to renamed paths
        """
        # Use provided batch_id or generate a default one
        if batch_id is None:
            batch_id = self.batch_prefix
        # Ensure batch_id follows the required format (bidXXXXXX)
        elif not batch_id.startswith(self.batch_prefix):
            # Extract any numeric part from the existing batch_id
            import re
            numeric_part = re.findall(r'\d+', batch_id)
            if numeric_part:
                # Use the numeric part with the prefix
                batch_id = f"{self.batch_prefix}{numeric_part[0]}"
            else:
                # If no numeric part, generate a timestamp-based ID
                import time
                batch_id = f"{self.batch_prefix}{int(time.time()) % 1000000:06d}"
            
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        renamed_files = {}
        
        # Sort files to ensure consistent ordering
        sorted_files = sorted(file_paths)
        
        print(f"Renaming {len(sorted_files)} files with batch ID '{batch_id}'...")
        
        # Process each file with sequential numbering
        for index, file_path in enumerate(sorted_files):
            sequence_number = index + 1  # Start from 1
            new_path = self.rename_file(file_path, output_dir, batch_id, sequence_number)
            
            if new_path:
                renamed_files[file_path] = new_path
                
                # Show progress periodically
                if (index + 1) % 100 == 0 or index == 0 or index == len(sorted_files) - 1:
                    print(f"Renamed {index + 1}/{len(sorted_files)} files...")
        
        print(f"Renaming complete. {len(renamed_files)} files renamed.")
        return renamed_files
