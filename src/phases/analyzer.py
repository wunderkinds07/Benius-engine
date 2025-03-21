# analyzer.py - Image analysis module

import os
from src.utils.image_utils import get_image_info, calculate_average_color

class Analyzer:
    """Analyzes image properties and characteristics"""
    
    def __init__(self, config):
        self.config = config
        self.min_resolution = config.get("min_resolution", 800)
        self.analyze_colors = config.get("analyze_colors", False)
    
    def analyze_image(self, image_path):
        """Analyze an image and extract its properties
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Dictionary with image properties (dimensions, format, etc.)
        """
        if not os.path.exists(image_path):
            return {
                "path": image_path,
                "error": "File not found"
            }
        
        # Get basic image info using utility function
        image_info = get_image_info(image_path)
        
        # Add additional analysis
        if "error" not in image_info:
            # Check if image meets minimum resolution
            image_info["meets_min_resolution"] = (
                image_info["width"] >= self.min_resolution and 
                image_info["height"] >= self.min_resolution
            )
            
            # Calculate aspect ratio class
            aspect_ratio = image_info.get("aspect_ratio", 0)
            if aspect_ratio > 0:
                # Classify the aspect ratio
                if 0.9 <= aspect_ratio <= 1.1:  # Allow for small deviations
                    image_info["aspect_type"] = "square"
                elif aspect_ratio > 1.1:
                    image_info["aspect_type"] = "landscape"
                else:  # aspect_ratio < 0.9
                    image_info["aspect_type"] = "portrait"
            
            # Calculate file size in MB
            image_info["size_mb"] = round(image_info["file_size"] / (1024 * 1024), 2)
            
            # Calculate average color if enabled
            if self.analyze_colors:
                avg_color = calculate_average_color(image_path)
                if avg_color:
                    image_info["avg_color"] = avg_color
        
        return image_info
    
    def process(self, file_paths):
        """Process a list of image files and analyze them
        
        Args:
            file_paths: List of image file paths or dictionary
            
        Returns:
            Dictionary mapping file paths to their analysis results
        """
        results = {}
        total_files = 0
        
        # Handle both list and dictionary inputs
        if isinstance(file_paths, dict):
            paths = list(file_paths.values())
        else:
            paths = file_paths
            
        total_files = len(paths)
        print(f"Analyzing {total_files} images...")
        
        # Process each file
        completed = 0
        for path in paths:
            results[path] = self.analyze_image(path)
            
            # Update progress periodically
            completed += 1
            if completed % 50 == 0 or completed == total_files:
                print(f"Analyzed {completed}/{total_files} images")
        
        # Calculate summary statistics
        stats = self.calculate_statistics(results)
        print("\nAnalysis complete. Summary:")
        print(f"  Total images: {stats['total']}")
        print(f"  Meet min resolution: {stats['meet_resolution']} ({stats['meet_resolution_pct']}%)")
        print(f"  Average dimensions: {stats['avg_width']}x{stats['avg_height']} px")
        print(f"  Average file size: {stats['avg_size_mb']} MB")
        
        return results
    
    def calculate_statistics(self, analysis_results):
        """Calculate summary statistics from analysis results
        
        Args:
            analysis_results: Dictionary of analysis results
            
        Returns:
            Dictionary with statistics
        """
        stats = {
            "total": len(analysis_results),
            "meet_resolution": 0,
            "failed": 0,
            "total_width": 0,
            "total_height": 0,
            "total_size": 0,
            "formats": {},
            "aspect_types": {}
        }
        
        for path, result in analysis_results.items():
            # Count errors
            if "error" in result:
                stats["failed"] += 1
                continue
                
            # Count images meeting resolution criteria
            if result.get("meets_min_resolution", False):
                stats["meet_resolution"] += 1
                
            # Sum dimensions and size for averages
            stats["total_width"] += result.get("width", 0)
            stats["total_height"] += result.get("height", 0)
            stats["total_size"] += result.get("file_size", 0)
            
            # Count formats
            format_name = result.get("format", "unknown")
            stats["formats"][format_name] = stats["formats"].get(format_name, 0) + 1
            
            # Count aspect ratio types
            aspect_type = result.get("aspect_type", "unknown")
            stats["aspect_types"][aspect_type] = stats["aspect_types"].get(aspect_type, 0) + 1
        
        # Calculate averages
        valid_count = stats["total"] - stats["failed"]
        stats["avg_width"] = round(stats["total_width"] / valid_count) if valid_count > 0 else 0
        stats["avg_height"] = round(stats["total_height"] / valid_count) if valid_count > 0 else 0
        stats["avg_size_mb"] = round(stats["total_size"] / (valid_count * 1024 * 1024), 2) if valid_count > 0 else 0
        stats["meet_resolution_pct"] = round((stats["meet_resolution"] / stats["total"]) * 100, 1) if stats["total"] > 0 else 0
        
        return stats
