#!/usr/bin/env python3
# tests/test_image_utils.py - Unit tests for image utilities

import os
import sys
import unittest
from pathlib import Path

# Add the src directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils.image_utils import is_valid_image, get_image_info, convert_to_webp

class TestImageUtils(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = Path(__file__).parent / 'test_data'
        self.test_dir.mkdir(exist_ok=True)
        
        # Create a simple test image if it doesn't exist
        self.test_image = self.test_dir / 'test_image.jpg'
        if not self.test_image.exists():
            try:
                from PIL import Image
                img = Image.new('RGB', (1024, 768), color='red')
                img.save(self.test_image)
            except ImportError:
                self.skipTest("PIL not available for creating test images")
                
    def tearDown(self):
        """Clean up test fixtures"""
        # We'll leave the test data for now, but you could add cleanup here
        pass
    
    def test_is_valid_image(self):
        """Test the is_valid_image function"""
        # Test with a valid image
        self.assertTrue(is_valid_image(self.test_image))
        
        # Test with a non-image file
        non_image = self.test_dir / 'not_an_image.txt'
        with open(non_image, 'w') as f:
            f.write("This is not an image")
        self.assertFalse(is_valid_image(non_image))
        
        # Test with a non-existent file
        self.assertFalse(is_valid_image(self.test_dir / 'nonexistent.jpg'))
    
    def test_get_image_info(self):
        """Test the get_image_info function"""
        info = get_image_info(self.test_image)
        
        # Verify the returned info has the expected fields
        self.assertIn('width', info)
        self.assertIn('height', info)
        self.assertIn('format', info)
        self.assertIn('size', info)
        
        # Verify the values are correct
        self.assertEqual(info['width'], 1024)
        self.assertEqual(info['height'], 768)
        self.assertEqual(info['format'].lower(), 'jpeg')
        
    def test_convert_to_webp(self):
        """Test the convert_to_webp function"""
        # Convert the test image to WebP
        output_path = self.test_dir / 'test_output.webp'
        result = convert_to_webp(self.test_image, output_path, quality=90)
        
        # Verify the conversion was successful
        self.assertTrue(result)
        self.assertTrue(output_path.exists())
        
        # Verify the converted image is valid
        self.assertTrue(is_valid_image(output_path))
        
        # Check the format of the converted image
        info = get_image_info(output_path)
        self.assertEqual(info['format'].lower(), 'webp')
        
        # Clean up
        output_path.unlink()

if __name__ == "__main__":
    unittest.main()
