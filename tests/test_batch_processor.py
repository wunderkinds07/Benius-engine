#!/usr/bin/env python3
# tests/test_batch_processor.py - Unit tests for batch processor

import os
import sys
import json
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add the src directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.batch_processor import BatchProcessor

class TestBatchProcessor(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = Path(__file__).parent / 'test_data'
        self.test_dir.mkdir(exist_ok=True)
        self.temp_dir = self.test_dir / 'temp'
        self.temp_dir.mkdir(exist_ok=True)
        self.output_dir = self.test_dir / 'output'
        self.output_dir.mkdir(exist_ok=True)
        
        # Create a test config
        self.config = {
            "min_resolution": 800,
            "output_format": "webp",
            "quality": 90,
            "batch_prefix": "test",
            "parallel_processing": False,  # Disable parallel processing for testing
            "database_path": str(self.test_dir / "test.db"),
            "output_directory": str(self.output_dir),
            "temp_directory": str(self.temp_dir),
            "resize_if_larger": True,
            "max_dimensions": (1024, 768)
        }
        
        # Initialize the processor
        self.processor = BatchProcessor(self.config)
    
    def tearDown(self):
        """Clean up test fixtures"""
        # Cleanup would go here if needed
        pass
    
    @patch('src.batch_processor.BatchProcessor._extract_phase')
    @patch('src.batch_processor.BatchProcessor._rename_phase')
    @patch('src.batch_processor.BatchProcessor._analyze_phase')
    @patch('src.batch_processor.BatchProcessor._filter_phase')
    @patch('src.batch_processor.BatchProcessor._convert_phase')
    @patch('src.batch_processor.BatchProcessor._package_phase')
    def test_execute_all_phases(self, mock_package, mock_convert, mock_filter, mock_analyze, mock_rename, mock_extract):
        """Test that all phases are executed in order"""
        # Set up mock returns
        mock_extract.return_value = ["file1.jpg", "file2.jpg"]
        mock_rename.return_value = {"file1.jpg": "test000001.jpg", "file2.jpg": "test000002.jpg"}
        mock_analyze.return_value = {"test000001.jpg": {"width": 1000, "height": 750}, "test000002.jpg": {"width": 800, "height": 600}}
        mock_filter.return_value = ["test000001.jpg", "test000002.jpg"]
        mock_convert.return_value = {"test000001.jpg": "test000001.webp", "test000002.jpg": "test000002.webp"}
        mock_package.return_value = "output.zip"
        
        # Execute the processor
        result = self.processor.execute("dummy_source")
        
        # Verify all phases were called in order
        mock_extract.assert_called_once()
        mock_rename.assert_called_once()
        mock_analyze.assert_called_once()
        mock_filter.assert_called_once()
        mock_convert.assert_called_once()
        mock_package.assert_called_once()
    
    def test_compare_batches(self):
        """Test batch comparison functionality"""
        # Create two fake batch results
        batch1 = {
            "batch_id": "batch1",
            "source": "source1.zip",
            "timestamp": "2025-03-21T10:00:00",
            "stats": {
                "total_images": 100,
                "processed_images": 80,
                "filtered_out": 20,
                "avg_width": 1200,
                "avg_height": 900,
                "formats": {"jpg": 60, "png": 40},
                "output_size": 15000000
            }
        }
        
        batch2 = {
            "batch_id": "batch2",
            "source": "source2.zip",
            "timestamp": "2025-03-21T11:00:00",
            "stats": {
                "total_images": 120,
                "processed_images": 100,
                "filtered_out": 20,
                "avg_width": 1400,
                "avg_height": 1000,
                "formats": {"jpg": 80, "png": 40},
                "output_size": 18000000
            }
        }
        
        # Save these as JSON files
        batch1_path = self.test_dir / "batch1_report.json"
        batch2_path = self.test_dir / "batch2_report.json"
        
        with open(batch1_path, 'w') as f:
            json.dump(batch1, f)
        
        with open(batch2_path, 'w') as f:
            json.dump(batch2, f)
        
        # Call the comparison function
        comparison = self.processor.compare_batches(str(batch1_path), str(batch2_path))
        
        # Verify comparison results
        self.assertIsNotNone(comparison)
        self.assertIn("batch1", comparison["batches"])
        self.assertIn("batch2", comparison["batches"])
        self.assertIn("comparison", comparison)
        
        # Check some comparison metrics
        diff = comparison["comparison"]
        self.assertEqual(diff["total_images_diff"], 20)  # 120 - 100
        self.assertEqual(diff["processed_images_diff"], 20)  # 100 - 80
        self.assertEqual(diff["filtered_out_diff"], 0)  # 20 - 20
        
        # Verify percentage changes
        self.assertIn("percent_changes", comparison["comparison"])
        percent = comparison["comparison"]["percent_changes"]
        self.assertAlmostEqual(percent["total_images"], 20.0)  # (120-100)/100 * 100
        self.assertAlmostEqual(percent["processed_images"], 25.0)  # (100-80)/80 * 100

if __name__ == "__main__":
    unittest.main()
