#!/usr/bin/env python3
# tests/test_converter.py - Unit tests for converter module

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add the src directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.phases.converter import Converter
from src.utils.image_utils import get_image_info

class TestConverter(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = Path(__file__).parent / 'test_data'
        self.test_dir.mkdir(exist_ok=True)
        self.output_dir = self.test_dir / 'converted'
        self.output_dir.mkdir(exist_ok=True)
        
        # Create test images with different sizes
        self.create_test_images()
        
        # Initialize converter with test settings
        self.config = {
            "output_format": "webp",
            "quality": 90,
            "preserve_metadata": True,
            "resize_if_larger": False,
            "max_dimensions": (1000, 800)
        }
        self.converter = Converter(self.config)
        
    def create_test_images(self):
        """Create test images with different sizes"""
        try:
            from PIL import Image
            
            # Standard size image (800x600)
            self.std_img = self.test_dir / 'standard.jpg'
            img = Image.new('RGB', (800, 600), color='red')
            img.save(self.std_img)
            
            # Large image (2000x1500)
            self.large_img = self.test_dir / 'large.jpg'
            img = Image.new('RGB', (2000, 1500), color='blue')
            img.save(self.large_img)
            
        except ImportError:
            self.skipTest("PIL not available for creating test images")
    
    def tearDown(self):
        """Clean up test fixtures"""
        # We'll leave the test data for now
        pass
    
    def test_convert_standard_image(self):
        """Test converting a standard size image"""
        # Convert the standard image
        converted_path = self.converter.convert_image(str(self.std_img), str(self.output_dir))
        
        # Check that conversion worked
        self.assertIsNotNone(converted_path)
        self.assertTrue(os.path.exists(converted_path))
        
        # Verify the format was changed to WebP
        info = get_image_info(converted_path)
        self.assertEqual(info['format'].lower(), 'webp')
        
        # Verify dimensions were preserved (no resizing)
        self.assertEqual(info['width'], 800)
        self.assertEqual(info['height'], 600)
    
    def test_convert_no_resize(self):
        """Test converting without resizing large images"""
        # Convert the large image without resize
        converted_path = self.converter.convert_image(str(self.large_img), str(self.output_dir))
        
        # Verify dimensions were preserved (no resizing)
        info = get_image_info(converted_path)
        self.assertEqual(info['width'], 2000)
        self.assertEqual(info['height'], 1500)
    
    def test_convert_with_resize(self):
        """Test converting with resize option enabled"""
        # Create a converter with resize enabled
        resize_config = self.config.copy()
        resize_config["resize_if_larger"] = True
        resize_converter = Converter(resize_config)
        
        # Convert the large image with resize
        converted_path = resize_converter.convert_image(str(self.large_img), str(self.output_dir))
        
        # Verify dimensions were reduced according to max_dimensions
        info = get_image_info(converted_path)
        self.assertLessEqual(info['width'], 1000)  # Should be scaled down
        self.assertLessEqual(info['height'], 800)  # Should be scaled down
        
        # Verify aspect ratio was preserved
        original_ratio = 2000 / 1500
        new_ratio = info['width'] / info['height']
        self.assertAlmostEqual(original_ratio, new_ratio, places=2)
    
    def test_batch_conversion(self):
        """Test batch conversion of multiple images"""
        # Create a list of images to convert
        images = [str(self.std_img), str(self.large_img)]
        
        # Convert all images
        results = self.converter.process(images, str(self.output_dir))
        
        # Verify all conversions succeeded
        self.assertEqual(len(results), 2)
        for original, converted in results.items():
            self.assertIsNotNone(converted)
            self.assertTrue(os.path.exists(converted))

if __name__ == "__main__":
    unittest.main()
