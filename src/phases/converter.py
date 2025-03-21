# converter.py - Image conversion module

import os
import time
from pathlib import Path

# Import utilities
from src.utils.image_utils import convert_image as utils_convert_image

class Converter:
    """Converts images to specified output format with quality settings"""
    
    def __init__(self, config):
        self.config = config
        self.output_format = config.get("output_format", "webp")
        self.quality = config.get("quality", 90)
        self.preserve_metadata = config.get("preserve_metadata", True)
        self.resize_if_larger = config.get("resize_if_larger", False)
        self.max_dimensions = config.get("max_dimensions", (3840, 2160))  # 4K default max
    
    def convert_image(self, image_path, output_dir):
        """Convert an image to the specified format
        
        Args:
            image_path: Path to the image file
            output_dir: Directory to save converted image
            
        Returns:
            Path to the converted image
        """
        try:
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            # Create output filename with new extension but preserve the basename
            # This maintains the batch ID and sequence number in the filename
            filename = os.path.basename(image_path)
            base_name = Path(filename).stem
            output_path = os.path.join(output_dir, f"{base_name}.{self.output_format}")
            
            # Convert the image using the utility function
            output_path = utils_convert_image(
                image_path, 
                output_path, 
                format=self.output_format, 
                quality=self.quality,
                preserve_metadata=self.preserve_metadata,
                resize_if_larger=self.resize_if_larger,
                max_dimensions=self.max_dimensions
            )
            
            return output_path
        except Exception as e:
            print(f"Error converting image {image_path}: {e}")
            return None
    
    def process(self, file_paths, output_dir):
        """Process a list of images and convert them
        
        Args:
            file_paths: List or dictionary of image file paths
            output_dir: Directory to save converted images
            
        Returns:
            Dictionary mapping original paths to converted paths
        """
        results = {}
        start_time = time.time()
        
        # Handle both list and dictionary inputs
        if isinstance(file_paths, dict):
            paths = list(file_paths.keys())
        else:
            paths = file_paths
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"Converting {len(paths)} images to {self.output_format.upper()} (quality: {self.quality})...")
        
        count = 0
        for path in paths:
            converted_path = self.convert_image(path, output_dir)
            results[path] = converted_path
            count += 1
            
            # Show progress every 10 images
            if count % 10 == 0:
                print(f"Converted {count}/{len(paths)} images...")
        
        elapsed = time.time() - start_time
        print(f"Conversion complete. {count} images converted in {elapsed:.2f} seconds.")
        
        return results
    
    def set_format(self, format_name):
        """Set the output format
        
        Args:
            format_name: Format to use (e.g., 'webp', 'jpeg', 'png')
            
        Returns:
            Self for method chaining
        """
        self.output_format = format_name.lower()
        return self
    
    def set_quality(self, quality):
        """Set the output quality
        
        Args:
            quality: Quality setting (1-100)
            
        Returns:
            Self for method chaining
        """
        self.quality = max(1, min(100, quality))  # Ensure quality is between 1 and 100
        return self
