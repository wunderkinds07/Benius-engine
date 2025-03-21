# packager.py - Image packaging module

import os
import zipfile
import time
from pathlib import Path

class Packager:
    """Packages processed images into compressed archives"""
    
    def __init__(self, config):
        self.config = config
        self.delete_after_packaging = config.get("delete_after_packaging", True)
        self.compression_level = config.get("compression_level", 9)  # 0-9, where 9 is highest
        self.include_metadata = config.get("include_metadata", True)
    
    def create_package(self, file_paths, output_path):
        """Create a ZIP archive containing the specified files
        
        Args:
            file_paths: List of file paths to include in the package
            output_path: Path to the output ZIP file
            
        Returns:
            Path to the created package
        """
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Create timestamp
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            
            # Add timestamp to filename if not already present
            if timestamp not in output_path:
                base_path = Path(output_path)
                # Insert timestamp before extension
                output_path = str(base_path.with_stem(f"{base_path.stem}_{timestamp}"))
            
            # Add .zip extension if missing
            if not output_path.lower().endswith(".zip"):
                output_path += ".zip"
            
            # Filter the file paths to only include existing files (not directories)
            valid_files = []
            for file_path in file_paths:
                if os.path.isfile(file_path):
                    valid_files.append(file_path)
                elif os.path.isdir(file_path):
                    # Skip directories
                    continue
                
            print(f"Packaging {len(valid_files)} files into {output_path}...")
            
            # Create ZIP archive
            with zipfile.ZipFile(output_path, 'w', compression=zipfile.ZIP_DEFLATED, 
                                compresslevel=self.compression_level) as zipf:
                # Add each file to the archive
                for file_path in valid_files:
                    if os.path.exists(file_path) and os.path.isfile(file_path):
                        # Use just the filename as the archive path to preserve the renamed format
                        # This ensures batch IDs and sequential numbers are preserved
                        archive_path = os.path.basename(file_path)
                        zipf.write(file_path, archive_path)
                
                # Add metadata if configured
                if self.include_metadata:
                    # Create a simple metadata text file
                    meta_content = f"Package created: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    meta_content += f"Files included: {len(valid_files)}\n"
                    meta_content += f"Compression level: {self.compression_level}\n"
                    
                    zipf.writestr("metadata.txt", meta_content)
            
            print(f"Packaging complete: {output_path}")
            return output_path
            
        except Exception as e:
            print(f"Error creating package: {e}")
            return None
    
    def cleanup_files(self, file_paths):
        """Delete original files after packaging if configured
        
        Args:
            file_paths: List of file paths to delete
            
        Returns:
            List of successfully deleted files
        """
        if not self.delete_after_packaging:
            return []
        
        deleted_files = []
        for file_path in file_paths:
            try:
                if os.path.exists(file_path) and os.path.isfile(file_path):
                    os.remove(file_path)
                    deleted_files.append(file_path)
            except Exception as e:
                print(f"Error deleting file {file_path}: {e}")
        
        if deleted_files:
            print(f"Cleaned up {len(deleted_files)} files after packaging")
            
        return deleted_files
    
    def process(self, file_paths, output_path):
        """Process and package a list of files
        
        Args:
            file_paths: List or dictionary of file paths to package
            output_path: Path to the output package
            
        Returns:
            Dictionary with package information
        """
        # Handle both list and dictionary inputs
        if isinstance(file_paths, dict):
            paths = list(file_paths.values())
        else:
            paths = file_paths
            
        start_time = time.time()
        package_path = self.create_package(paths, output_path)
        
        if package_path:
            # Get package size
            package_size = os.path.getsize(package_path) if os.path.exists(package_path) else 0
            package_size_mb = round(package_size / (1024 * 1024), 2)
            
            # Cleanup if configured
            deleted_files = self.cleanup_files(paths) if self.delete_after_packaging else []
            
            elapsed = time.time() - start_time
            print(f"Packaging completed in {elapsed:.2f} seconds")
            print(f"Package size: {package_size_mb} MB")
            
            return {
                "package_path": package_path,
                "file_count": len(paths),
                "deleted_files": deleted_files,
                "package_size": package_size,
                "package_size_mb": package_size_mb,
                "elapsed_seconds": elapsed
            }
        else:
            return {
                "package_path": None,
                "file_count": len(paths),
                "error": "Failed to create package"
            }
