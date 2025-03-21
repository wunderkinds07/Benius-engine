# extractor.py - Image extraction module

import os
import zipfile
import tarfile
import shutil
from pathlib import Path
import time
from tqdm import tqdm

# Import utility modules
from src.utils.string_utils import is_image_file
from src.utils.image_utils import is_valid_image

class Extractor:
    """Handles extraction of images from various source formats (TAR, ZIP, HuggingFace)"""
    
    def __init__(self, config):
        self.config = config
        self.valid_extensions = config.get("valid_image_extensions", ["jpg", "jpeg", "png", "gif", "bmp", "webp", "tiff"])
        self.extract_nested = config.get("extract_nested_archives", False)
        # If HuggingFace utils are available, initialize the HF manager
        try:
            from src.utils.huggingface_utils import HuggingFaceManager
            self.hf_manager = HuggingFaceManager(config)
            self.hf_available = True
        except (ImportError, ModuleNotFoundError):
            self.hf_available = False
    
    def extract_from_archive(self, archive_path, output_dir):
        """Extract images from TAR or ZIP archives
        
        Args:
            archive_path: Path to the archive file
            output_dir: Directory to extract files to
            
        Returns:
            List of extracted file paths
        """
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        extracted_files = []
        archive_path = os.path.abspath(archive_path)
        
        # Determine archive type and extract accordingly
        if archive_path.lower().endswith(('.zip')):
            extracted_files = self._extract_zip(archive_path, output_dir)
        elif archive_path.lower().endswith(('.tar', '.tar.gz', '.tgz')):
            extracted_files = self._extract_tar(archive_path, output_dir)
        else:
            raise ValueError(f"Unsupported archive format: {archive_path}")
        
        # Filter out non-image files
        image_files = [path for path in extracted_files if is_image_file(path, self.valid_extensions)]
        
        # Validate images (check if they can be opened)
        valid_images = []
        for img_path in image_files:
            if is_valid_image(img_path):
                valid_images.append(img_path)
            else:
                print(f"Warning: Invalid or corrupt image file: {img_path}")
        
        return valid_images
    
    def _extract_zip(self, zip_path, output_dir):
        """Extract files from a ZIP archive
        
        Args:
            zip_path: Path to the ZIP archive
            output_dir: Directory to extract files to
            
        Returns:
            List of extracted file paths
        """
        extracted_files = []
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Get list of files in archive
                file_list = zip_ref.namelist()
                
                # Extract files
                for file_path in file_list:
                    # Skip directories
                    if file_path.endswith('/'):
                        continue
                        
                    # Extract only image files or all files if extract_nested is True
                    if self.extract_nested or is_image_file(file_path, self.valid_extensions):
                        zip_ref.extract(file_path, output_dir)
                        extracted_path = os.path.join(output_dir, file_path)
                        extracted_files.append(extracted_path)
                        
                        # Process nested archives if enabled
                        if self.extract_nested and extracted_path.lower().endswith(('.zip', '.tar', '.tar.gz', '.tgz')):
                            nested_dir = os.path.join(output_dir, f"nested_{Path(file_path).stem}")
                            nested_files = self.extract_from_archive(extracted_path, nested_dir)
                            extracted_files.extend(nested_files)
                            # Remove the nested archive after extraction
                            os.remove(extracted_path)
        except zipfile.BadZipFile:
            raise ValueError(f"Invalid or corrupt ZIP file: {zip_path}")
        
        return extracted_files
    
    def _extract_tar(self, tar_path, output_dir):
        """Extract files from a TAR archive
        
        Args:
            tar_path: Path to the TAR archive
            output_dir: Directory to extract files to
            
        Returns:
            List of extracted file paths
        """
        extracted_files = []
        
        try:
            # First, get an estimate of the archive size
            archive_size = os.path.getsize(tar_path)
            print(f"Extracting TAR archive: {tar_path} ({archive_size / (1024*1024):.1f} MB)")
            print("This may take some time for large archives. Please be patient...")
            
            # Show progress during extraction
            progress_bar = None
            read_bytes = 0
            last_update = time.time()
            update_interval = 0.5  # seconds
            
            with tarfile.open(tar_path, 'r:*') as tar_ref:
                # Try to get file count (this might be slow for large archives)
                try:
                    file_list = tar_ref.getnames()
                    file_count = len(file_list)
                    print(f"Archive contains {file_count} files")
                    progress_bar = tqdm(total=file_count, desc="Extracting files")
                except Exception as e:
                    print(f"Could not get file count: {e}")
                    print("Extracting files with progress updates...")
                    file_list = []
                
                # Create a custom extractor to show progress
                def custom_extract(member):
                    nonlocal read_bytes, last_update
                    if member.isreg():  # Only extract regular files
                        tar_ref.extract(member, path=output_dir)
                        read_bytes += member.size
                        
                        # Update progress only every update_interval seconds to avoid slowdown
                        current_time = time.time()
                        if current_time - last_update > update_interval:
                            print(f"\rExtracted: {read_bytes / (1024*1024):.1f} MB", end="")
                            last_update = current_time
                            
                        # If we have a progress bar, update it
                        if progress_bar:
                            progress_bar.update(1)
                            
                        # Add to extracted files list
                        extracted_path = os.path.join(output_dir, member.name)
                        extracted_files.append(extracted_path)
                        
                        # Process nested archives if enabled
                        if self.extract_nested and extracted_path.lower().endswith(('.zip', '.tar', '.tar.gz', '.tgz')):
                            try:
                                nested_dir = os.path.join(output_dir, f"nested_{Path(member.name).stem}")
                                nested_files = self.extract_from_archive(extracted_path, nested_dir)
                                extracted_files.extend(nested_files)
                                # Remove the nested archive after extraction
                                os.remove(extracted_path)
                            except Exception as nested_err:
                                print(f"Error extracting nested archive {extracted_path}: {nested_err}")
                    return member
                
                # Extract all members with progress updates
                if file_list:
                    for member in file_list:
                        member_obj = tar_ref.getmember(member)
                        custom_extract(member_obj)
                else:
                    # If we couldn't get the file list, extract everything
                    for member in tqdm(tar_ref.getmembers(), desc="Extracting files"):
                        custom_extract(member)
                        
            # Clean up progress bar if it exists        
            if progress_bar:
                progress_bar.close()
            
            print(f"\nExtraction complete. Extracted {len(extracted_files)} files")
            return extracted_files
            
        except tarfile.ReadError as e:
            print(f"Error: Could not read TAR file: {e}")
            raise ValueError(f"Invalid or corrupt TAR file: {tar_path}")
        except Exception as e:
            print(f"Error extracting from TAR: {e}")
            raise
    
    def extract_from_directory(self, directory_path, output_dir):
        """Copy images from a directory to the output directory
        
        Args:
            directory_path: Path to the directory containing images
            output_dir: Directory to copy images to
            
        Returns:
            List of copied image file paths
        """
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        copied_files = []
        directory_path = os.path.abspath(directory_path)
        
        # Walk through directory
        for root, _, files in os.walk(directory_path):
            for file in files:
                # Check if file is an image
                if is_image_file(file, self.valid_extensions):
                    source_path = os.path.join(root, file)
                    
                    # Validate image
                    if is_valid_image(source_path):
                        # Create relative path structure in output directory
                        rel_path = os.path.relpath(root, directory_path)
                        dest_dir = os.path.join(output_dir, rel_path)
                        os.makedirs(dest_dir, exist_ok=True)
                        
                        # Copy file
                        dest_path = os.path.join(dest_dir, file)
                        shutil.copy2(source_path, dest_path)
                        copied_files.append(dest_path)
                    else:
                        print(f"Warning: Invalid or corrupt image file: {source_path}")
        
        return copied_files
    
    def extract_from_huggingface(self, dataset_name, output_dir):
        """Extract images from HuggingFace dataset
        
        Args:
            dataset_name: Name of the HuggingFace dataset
            output_dir: Directory to store extracted images
            
        Returns:
            List of extracted file paths
        """
        if not self.hf_available:
            raise ImportError("HuggingFace integration is not available. Install the 'datasets' package.")
            
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Load dataset
        dataset = self.hf_manager.load_dataset(dataset_name)
        if not dataset:
            raise ValueError(f"Failed to load HuggingFace dataset: {dataset_name}")
        
        # Extract images
        image_column = self.config.get("huggingface_image_column", "image")
        extracted_paths = self.hf_manager.extract_images(dataset, image_column=image_column, output_dir=output_dir)
        
        return extracted_paths
    
    def process(self, source_path, output_dir):
        """Main processing function that handles the extraction
        
        Args:
            source_path: Path to the source file or directory
            output_dir: Directory to extract files to
            
        Returns:
            List of extracted file paths
        """
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Process based on source type
        if os.path.isfile(source_path):
            # Source is a file, check if it's an archive or single image
            if is_image_file(source_path, self.valid_extensions):
                # Single image file
                return self.copy_single_file(source_path, output_dir)
            else:
                # Archive file
                extracted_files = self.extract_from_archive(source_path, output_dir)
                
                # Apply sample mode if enabled
                if self.config.get("sample_mode", False):
                    sample_size = min(self.config.get("sample_size", 100), len(extracted_files))
                    is_random = self.config.get("sample_random", True)
                    
                    print(f"Sample mode: Selecting {sample_size} images from {len(extracted_files)} extracted files")
                    
                    if is_random:
                        # Random sampling
                        import random
                        sampled_files = random.sample(extracted_files, sample_size) if len(extracted_files) > sample_size else extracted_files
                    else:
                        # Sequential sampling (first N)
                        sampled_files = extracted_files[:sample_size]
                    
                    # Delete non-sampled files to save space
                    deleted_count = 0
                    for file_path in extracted_files:
                        if file_path not in sampled_files and os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                                deleted_count += 1
                            except Exception as e:
                                print(f"Warning: Could not delete non-sampled file {file_path}: {e}")
                    
                    print(f"Removed {deleted_count} non-sampled files, keeping {len(sampled_files)} for processing")
                    return sampled_files
                
                return extracted_files
                
        elif os.path.isdir(source_path):
            # Source is a directory
            return self.extract_from_directory(source_path, output_dir)
        elif source_path.startswith("hf://") and self.hf_available:
            # HuggingFace dataset
            dataset_name = source_path[5:]
            return self.hf_manager.extract_dataset(dataset_name, output_dir)
        else:
            raise ValueError(f"Invalid source path: {source_path}")
