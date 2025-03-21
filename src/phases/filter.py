# filter.py - Image filtering module

import os
import shutil
from PIL import Image

class Filter:
    """Filters images based on criteria like dimensions and quality"""
    
    def __init__(self, config):
        self.config = config
        self.min_resolution = config.get("min_resolution", 800)
    
    def meets_criteria(self, image_path):
        """Check if an image meets the filtering criteria
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Boolean indicating if image meets criteria
        """
        try:
            # Open the image to get dimensions
            with Image.open(image_path) as img:
                width, height = img.size
                
                # Check if dimensions meet minimum resolution requirement
                return width >= self.min_resolution and height >= self.min_resolution
        except Exception as e:
            print(f"Error checking image criteria for {image_path}: {e}")
            return False
    
    def process(self, file_paths, output_dir):
        """Filter images based on resolution criteria
        
        Args:
            file_paths: List of image file paths to process
            output_dir: Directory to copy filtered images to
            
        Returns:
            List of filtered image paths in the output directory
        """
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        filtered_files = []
        rejected_count = 0
        
        print(f"Filtering {len(file_paths)} images with minimum resolution {self.min_resolution}px...")
        
        for i, file_path in enumerate(file_paths):
            # Show progress every 10 files
            if i % 10 == 0 or i == len(file_paths) - 1:
                print(f"Filtered {i+1}/{len(file_paths)} images...")
                
            # Check if the image meets our criteria
            if self.meets_criteria(file_path):
                # Copy to output directory
                filename = os.path.basename(file_path)
                output_path = os.path.join(output_dir, filename)
                shutil.copy2(file_path, output_path)
                filtered_files.append(output_path)
            else:
                rejected_count += 1
        
        print(f"Filtering complete. {len(filtered_files)} images passed, {rejected_count} images rejected.")
        return filtered_files
