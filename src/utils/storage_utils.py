# storage_utils.py - Storage management utilities

import os
import shutil
import zipfile
import datetime
from pathlib import Path

class StorageManager:
    """Manages storage operations for image processing"""
    
    def __init__(self, config):
        self.config = config
        self.temp_dir = config.get("temp_directory", "data/temp")
        self.output_dir = config.get("output_directory", "output")
        self.delete_after_packaging = config.get("delete_after_packaging", True)
        
        # Ensure directories exist
        os.makedirs(self.temp_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
    
    def create_temp_directory(self, prefix="batch_"):
        """Create a temporary directory for processing
        
        Args:
            prefix: Prefix for the directory name
            
        Returns:
            Path to the created directory
        """
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_dir = os.path.join(self.temp_dir, f"{prefix}{timestamp}")
        os.makedirs(temp_dir, exist_ok=True)
        return temp_dir
    
    def cleanup_temp_files(self, directory=None):
        """Clean up temporary files and directories
        
        Args:
            directory: Specific directory to clean (None for all temp files)
            
        Returns:
            Boolean indicating success
        """
        try:
            if directory and os.path.exists(directory):
                shutil.rmtree(directory)
            elif directory is None and os.path.exists(self.temp_dir):
                # Remove all contents but keep the directory
                for item in os.listdir(self.temp_dir):
                    item_path = os.path.join(self.temp_dir, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)
            return True
        except Exception as e:
            print(f"Error cleaning up temp files: {e}")
            return False
    
    def package_files(self, file_paths, output_path, compression=zipfile.ZIP_DEFLATED):
        """Package files into a ZIP archive
        
        Args:
            file_paths: List of files to package
            output_path: Path for the output package
            compression: Compression method
            
        Returns:
            Dictionary with package information
        """
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        try:
            # Create ZIP file
            with zipfile.ZipFile(output_path, 'w', compression=compression) as zipf:
                # Add each file to the ZIP
                file_count = 0
                for file_path in file_paths:
                    if os.path.exists(file_path):
                        # Use just the filename to avoid directory structure in ZIP
                        arcname = os.path.basename(file_path)
                        zipf.write(file_path, arcname=arcname)
                        file_count += 1
            
            # Get package size
            package_size = os.path.getsize(output_path) if os.path.exists(output_path) else 0
            
            return {
                "package_path": output_path,
                "file_count": file_count,
                "package_size": package_size
            }
        except Exception as e:
            print(f"Error packaging files: {e}")
            return {
                "package_path": None,
                "file_count": 0,
                "package_size": 0,
                "error": str(e)
            }
    
    def create_directory_structure(self, batch_id):
        """Create directory structure for a batch
        
        Args:
            batch_id: Unique identifier for the batch
            
        Returns:
            Dictionary with paths to created directories
        """
        # Create batch directories
        batch_dir = os.path.join(self.temp_dir, batch_id)
        extract_dir = os.path.join(batch_dir, "extracted")
        renamed_dir = os.path.join(batch_dir, "renamed")
        converted_dir = os.path.join(batch_dir, "converted")
        
        os.makedirs(batch_dir, exist_ok=True)
        os.makedirs(extract_dir, exist_ok=True)
        os.makedirs(renamed_dir, exist_ok=True)
        os.makedirs(converted_dir, exist_ok=True)
        
        return {
            "batch_dir": batch_dir,
            "extract_dir": extract_dir,
            "renamed_dir": renamed_dir,
            "converted_dir": converted_dir
        }
    
    def cleanup_after_packaging(self, dirs_to_clean):
        """Clean up directories after packaging if configured to do so
        
        Args:
            dirs_to_clean: List of directories to clean up
            
        Returns:
            Boolean indicating success
        """
        if not self.delete_after_packaging:
            return False
            
        try:
            for directory in dirs_to_clean:
                if os.path.exists(directory):
                    shutil.rmtree(directory)
            return True
        except Exception as e:
            print(f"Error cleaning up after packaging: {e}")
            return False
