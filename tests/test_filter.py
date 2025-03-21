#!/usr/bin/env python3
# tests/test_filter.py - Unit tests for filter module

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add the src directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.phases.filter import Filter

class TestFilter(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = Path(__file__).parent / 'test_data'
        self.test_dir.mkdir(exist_ok=True)
        self.output_dir = self.test_dir / 'filtered'
        self.output_dir.mkdir(exist_ok=True)
        
        # Create test images with different resolutions
        self.create_test_images()
        
        # Initialize filter with test settings
        self.filter = Filter(min_resolution=800)
        
    def create_test_images(self):
        """Create test images with different resolutions"""
        try:
            from PIL import Image
            
            # High resolution image (1024x768)
            self.high_res_img = self.test_dir / 'high_res.jpg'
            img = Image.new('RGB', (1024, 768), color='red')
            img.save(self.high_res_img)
            
            # Low resolution image (640x480)
            self.low_res_img = self.test_dir / 'low_res.jpg'
            img = Image.new('RGB', (640, 480), color='blue')
            img.save(self.low_res_img)
            
            # Edge case image (800x600) - exactly at the threshold
            self.edge_img = self.test_dir / 'edge_case.jpg'
            img = Image.new('RGB', (800, 600), color='green')
            img.save(self.edge_img)
            
            # Portrait image (600x900) - one dimension below, one above
            self.portrait_img = self.test_dir / 'portrait.jpg'
            img = Image.new('RGB', (600, 900), color='yellow')
            img.save(self.portrait_img)
            
        except ImportError:
            self.skipTest("PIL not available for creating test images")
    
    def tearDown(self):
        """Clean up test fixtures"""
        # We'll leave the test data for now
        pass
    
    def test_filter_images(self):
        """Test filtering images based on resolution"""
        test_images = [
            self.high_res_img,
            self.low_res_img,
            self.edge_img,
            self.portrait_img
        ]
        
        # Mock the progress manager
        mock_progress = MagicMock()
        
        # Run the filter
        filtered_images = self.filter.filter_images(
            test_images, 
            self.output_dir,
            progress_manager=mock_progress
        )
        
        # We expect high_res_img and edge_img to pass the filter
        # The portrait_img should be filtered out because one dimension is below threshold
        self.assertEqual(len(filtered_images), 2)
        
        # Check that the right files are in the filtered list
        filtered_filenames = [Path(img).name for img in filtered_images]
        self.assertIn('high_res.jpg', filtered_filenames)
        self.assertIn('edge_case.jpg', filtered_filenames)
        self.assertNotIn('low_res.jpg', filtered_filenames)
        self.assertNotIn('portrait.jpg', filtered_filenames)
        
        # Verify the progress manager was used correctly
        mock_progress.start_phase.assert_called_once()
        self.assertEqual(mock_progress.update.call_count, 4)  # One call per image
        mock_progress.finish_phase.assert_called_once()
    
    def test_custom_min_resolution(self):
        """Test with a custom minimum resolution"""
        # Create a filter with a higher minimum resolution
        high_filter = Filter(min_resolution=900)
        
        test_images = [
            self.high_res_img,
            self.low_res_img,
            self.edge_img,
            self.portrait_img
        ]
        
        # Run the filter without progress manager
        filtered_images = high_filter.filter_images(test_images, self.output_dir)
        
        # Only the high_res_img should pass with the higher threshold
        self.assertEqual(len(filtered_images), 1)
        self.assertEqual(Path(filtered_images[0]).name, 'high_res.jpg')

if __name__ == "__main__":
    unittest.main()
