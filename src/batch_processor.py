# batch_processor.py - Main batch processing orchestrator

import os
import time
import json
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import random
import shutil

# Import phase modules
from src.phases.extractor import Extractor
from src.phases.renamer import Renamer
from src.phases.analyzer import Analyzer
from src.phases.filter import Filter
from src.phases.converter import Converter
from src.phases.packager import Packager

# Import utility modules
from src.utils.database_utils import DatabaseManager
from src.utils.report_utils import ReportGenerator
from src.utils.progress_utils import ProgressManager
from src.utils.string_utils import generate_unique_id
from src.utils.checkpoint_utils import CheckpointManager
from src.utils.parallel_utils import ParallelProcessor
from src.utils.storage_utils import StorageManager
from src.utils.memory_utils import MemoryOptimizer

class BatchProcessor:
    """Main orchestrator for the image processing pipeline"""
    
    def __init__(self, config):
        self.config = config
        
        # Initialize components
        self.extractor = Extractor(config)
        self.renamer = Renamer(config)
        self.analyzer = Analyzer(config)
        self.filter = Filter(config)
        self.converter = Converter(config)
        self.packager = Packager(config)
        
        # Initialize utilities
        self.db_manager = DatabaseManager(config)
        self.report_generator = ReportGenerator(config)
        self.progress_manager = ProgressManager(config)
        self.checkpoint_manager = CheckpointManager(config)
        self.parallel_processor = ParallelProcessor(config)
        self.storage_manager = StorageManager(config)
        
        # Setup directories
        self.temp_dir = config.get("temp_directory", "data/temp")
        self.output_dir = config.get("output_directory", "output")
        self.checkpoint_dir = config.get("checkpoint_directory", "checkpoints")
        os.makedirs(self.temp_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        # Parallel processing config
        self.parallel = config.get("parallel_processing", True)
        self.max_workers = config.get("max_workers", 8)
        
        # Checkpointing config
        self.use_checkpointing = config.get("use_checkpointing", True)
        self.checkpoint_interval = config.get("checkpoint_interval", 10)  # minutes
        
        # Error handling
        self.continue_on_error = config.get("continue_on_error", True)
    
    def process(self, source_path, resume_from=None):
        """Process a batch of images from a source
        
        Args:
            source_path: Path to the source file or directory
            resume_from: Optional checkpoint to resume from
            
        Returns:
            Dictionary with processing results
        """
        # Generate a batch ID
        batch_id = f"batch{random.randint(10000000, 99999999)}"
        print(f"Processing batch {batch_id} from {source_path}")
        
        # Create directories for processing
        extract_dir = os.path.join(self.config["temp_directory"], f"{batch_id}_extracted")
        renamed_dir = os.path.join(self.config["temp_directory"], f"{batch_id}_renamed")
        filtered_dir = os.path.join(self.config["temp_directory"], f"{batch_id}_filtered")
        converted_dir = os.path.join(self.config["temp_directory"], f"{batch_id}_converted")
        
        # Initialize results dictionary
        results = {
            "batch_id": batch_id,
            "source": source_path,
            "timestamp": datetime.now().isoformat(),
            "stats": {}
        }
        
        try:
            # EXTRACTION PHASE
            state_data = {"phase": "extract", "status": "started"}
            self.checkpoint_manager.save_checkpoint(batch_id, state_data)
            extracted_files = self.extractor.process(source_path, extract_dir)
            extract_count = len(extracted_files) if extracted_files else 0
            results["stats"]["extracted"] = extract_count
            state_data = {"phase": "extract", "status": "completed", "count": extract_count}
            self.checkpoint_manager.save_checkpoint(batch_id, state_data)
            
            if not extracted_files or len(extracted_files) == 0:
                print(f"No valid images extracted from {source_path}")
                return {"error": "No valid images found in source"}
            
            # Use memory optimization for large batches
            memory_optimizer = MemoryOptimizer(self.config)
            batch_size = self.config.get("memory_batch_size", 100)
            
            # Process in batches to avoid memory issues
            total_renamed = 0
            total_filtered = 0
            total_converted = 0
            total_packaged = 0
            
            # Process in memory-efficient batches
            for batch_idx, file_batch in enumerate(memory_optimizer.batch_generator(extracted_files)):
                print(f"Processing batch {batch_idx+1} of {(len(extracted_files) + batch_size - 1) // batch_size}")
                
                # RENAMING PHASE
                state_data = {"phase": f"rename_batch_{batch_idx}", "status": "started"}
                self.checkpoint_manager.save_checkpoint(batch_id, state_data)
                renamed_files = self.renamer.process(file_batch, renamed_dir, batch_id)
                rename_count = len(renamed_files) if renamed_files else 0
                total_renamed += rename_count
                state_data = {"phase": f"rename_batch_{batch_idx}", "status": "completed", "count": rename_count}
                self.checkpoint_manager.save_checkpoint(batch_id, state_data)
                
                # FILTERING PHASE
                state_data = {"phase": f"filter_batch_{batch_idx}", "status": "started"}
                self.checkpoint_manager.save_checkpoint(batch_id, state_data)
                filtered_files = self.filter.process(renamed_files, filtered_dir)
                filter_count = len(filtered_files) if filtered_files else 0
                total_filtered += filter_count
                state_data = {"phase": f"filter_batch_{batch_idx}", "status": "completed", "count": filter_count}
                self.checkpoint_manager.save_checkpoint(batch_id, state_data)
                
                # CONVERSION PHASE
                state_data = {"phase": f"convert_batch_{batch_idx}", "status": "started"}
                self.checkpoint_manager.save_checkpoint(batch_id, state_data)
                converted_files = self.converter.process(filtered_files, converted_dir)
                convert_count = len(converted_files) if converted_files else 0
                total_converted += convert_count
                state_data = {"phase": f"convert_batch_{batch_idx}", "status": "completed", "count": convert_count}
                self.checkpoint_manager.save_checkpoint(batch_id, state_data)
                
                # Free up memory after each batch
                memory_optimizer.optimize_memory(force=True)
            
            # Update overall stats    
            results["stats"]["renamed"] = total_renamed
            results["stats"]["filtered"] = total_filtered
            results["stats"]["converted"] = total_converted
                
            # PACKAGING PHASE
            state_data = {"phase": "package", "status": "started"}
            self.checkpoint_manager.save_checkpoint(batch_id, state_data)
            
            # Create output package path
            package_filename = f"{batch_id}_processed.zip"
            package_path = os.path.join(self.output_dir, package_filename)
            
            # Collect all files from the converted directory for packaging
            converted_files = []
            if os.path.exists(converted_dir) and os.path.isdir(converted_dir):
                for root, _, files in os.walk(converted_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        converted_files.append(file_path)
            
            print(f"Found {len(converted_files)} files to package")
            
            # Package the files
            package_result = self.packager.process(converted_files, package_path)
            results["package"] = package_result
            results["stats"]["packaged"] = total_converted
            state_data = {"phase": "package", "status": "completed", "path": package_result}
            self.checkpoint_manager.save_checkpoint(batch_id, state_data)
            
            # Generate JSON report
            report_path = os.path.join(self.config["report_directory"], f"{batch_id}_report.json")
            os.makedirs(os.path.dirname(report_path), exist_ok=True)
            with open(report_path, 'w') as f:
                json.dump(results, f, indent=2)
            
            print(f"\nBatch {batch_id} processing complete")
            print(f"Extracted: {results['stats']['extracted']} images")
            print(f"Renamed: {results['stats']['renamed']} images")
            print(f"Filtered: {results['stats']['filtered']} images")
            print(f"Converted: {results['stats']['converted']} images")
            print(f"Output package: {package_path}")
            print(f"Report: {report_path}")
            
            return results
        except Exception as e:
            print(f"Error processing batch: {str(e)}")
            traceback.print_exc()
            error_data = {"phase": "error", "error": str(e)}
            self.checkpoint_manager.save_checkpoint(batch_id, error_data)
            return {"error": str(e)}
        finally:
            # Clean up temporary directories if configured to do so
            if self.config.get("delete_after_packaging", True):
                for dir_path in [extract_dir, renamed_dir, filtered_dir, converted_dir]:
                    if os.path.exists(dir_path):
                        try:
                            shutil.rmtree(dir_path)
                        except Exception as e:
                            print(f"Warning: Could not remove temporary directory {dir_path}: {e}")
    
    def _initialize_results(self, batch_id, source_path):
        """Initialize the results tracking dictionary
        
        Args:
            batch_id: Batch identifier
            source_path: Source path being processed
            
        Returns:
            Initialized results dictionary
        """
        return {
            "batch_id": batch_id,
            "source": source_path,
            "start_time": time.time(),
            "extracted_files": 0,
            "renamed_files": 0,
            "analyzed_files": 0,
            "accepted_files": 0,
            "rejected_files": 0,
            "converted_files": 0,
            "packaged_files": 0,
            "elapsed_time": 0
        }
    
    def _save_checkpoint(self, batch_id, current_phase, results):
        """Save processing checkpoint
        
        Args:
            batch_id: Batch identifier
            current_phase: Current processing phase
            results: Current results dictionary
            
        Returns:
            Path to saved checkpoint file or None
        """
        if not self.use_checkpointing:
            return None
            
        checkpoint_data = {
            "batch_id": batch_id,
            "current_phase": current_phase,
            "results": results,
            "timestamp": time.time()
        }
        
        return self.checkpoint_manager.save_checkpoint(batch_id, checkpoint_data)
    
    def _get_files_from_directory(self, directory):
        """Get a list of files from a directory
        
        Args:
            directory: Directory to scan
            
        Returns:
            List of file paths
        """
        if not os.path.exists(directory):
            return []
            
        return [os.path.join(directory, f) for f in os.listdir(directory) 
                if os.path.isfile(os.path.join(directory, f))]
    
    def execute(self, source_path, resume_from=None):
        """Execute the batch processing
        
        Args:
            source_path: Path to source (archive or HuggingFace dataset name)
            resume_from: Optional checkpoint to resume from
            
        Returns:
            Dictionary with processing results
        """
        return self.process(source_path, resume_from)
    
    def package(self, file_paths, metadata=None):
        """Package the processed files
        
        Args:
            file_paths: List of files to package
            metadata: Optional metadata to include in the package
            
        Returns:
            Path to the packaged file
        """
        packager = Packager(self.config)
        result = packager.package_files(file_paths, metadata=metadata)
        return result
        
    def compare_batches(self, batch1_report, batch2_report):
        """Compare two batch reports and highlight differences
        
        Args:
            batch1_report: Path to the first batch report JSON
            batch2_report: Path to the second batch report JSON
            
        Returns:
            Dictionary with comparison data
        """
        try:
            # Load reports
            with open(batch1_report, 'r') as f1:
                batch1 = json.load(f1)
                
            with open(batch2_report, 'r') as f2:
                batch2 = json.load(f2)
                
            # Validate reports format
            required_keys = ["batch_id", "stats"]
            for key in required_keys:
                if key not in batch1 or key not in batch2:
                    raise ValueError(f"Invalid report format: missing '{key}' key")
                    
            # Extract stats for comparison
            stats1 = batch1["stats"]
            stats2 = batch2["stats"]
            
            # Calculate differences
            comparison = {
                "batches": {
                    batch1["batch_id"]: batch1,
                    batch2["batch_id"]: batch2
                },
                "comparison": {
                    "timestamp": datetime.now().isoformat(),
                }
            }
            
            # Compare numeric stats
            numeric_stats = [
                "total_images", "processed_images", "filtered_out", 
                "avg_width", "avg_height", "output_size"
            ]
            
            # Calculate absolute differences
            for stat in numeric_stats:
                if stat in stats1 and stat in stats2:
                    diff_key = f"{stat}_diff"
                    comparison["comparison"][diff_key] = stats2[stat] - stats1[stat]
            
            # Calculate percentage changes
            percent_changes = {}
            for stat in numeric_stats:
                if stat in stats1 and stat in stats2 and stats1[stat] > 0:
                    percent = ((stats2[stat] - stats1[stat]) / stats1[stat]) * 100
                    percent_changes[stat] = round(percent, 2)
            
            comparison["comparison"]["percent_changes"] = percent_changes
            
            # Compare format distributions
            if "formats" in stats1 and "formats" in stats2:
                format_diff = {}
                # Combine all formats from both batches
                all_formats = set(stats1["formats"].keys()) | set(stats2["formats"].keys())
                
                for fmt in all_formats:
                    count1 = stats1["formats"].get(fmt, 0)
                    count2 = stats2["formats"].get(fmt, 0)
                    format_diff[fmt] = count2 - count1
                
                comparison["comparison"]["format_differences"] = format_diff
            
            # Generate summary text
            summary = []
            if "total_images_diff" in comparison["comparison"]:
                diff = comparison["comparison"]["total_images_diff"]
                if diff > 0:
                    summary.append(f"Batch {batch2['batch_id']} has {abs(diff)} more images than {batch1['batch_id']}")
                elif diff < 0:
                    summary.append(f"Batch {batch2['batch_id']} has {abs(diff)} fewer images than {batch1['batch_id']}")
            
            if "processed_images_diff" in comparison["comparison"]:
                diff = comparison["comparison"]["processed_images_diff"]
                if diff > 0:
                    summary.append(f"Batch {batch2['batch_id']} processed {abs(diff)} more images")
                elif diff < 0:
                    summary.append(f"Batch {batch2['batch_id']} processed {abs(diff)} fewer images")
            
            if "avg_width_diff" in comparison["comparison"] and "avg_height_diff" in comparison["comparison"]:
                width_diff = comparison["comparison"]["avg_width_diff"]
                height_diff = comparison["comparison"]["avg_height_diff"]
                if width_diff > 0 or height_diff > 0:
                    summary.append(f"Batch {batch2['batch_id']} has larger average image dimensions")
                elif width_diff < 0 or height_diff < 0:
                    summary.append(f"Batch {batch2['batch_id']} has smaller average image dimensions")
            
            comparison["comparison"]["summary"] = summary
            
            # Generate and save comparison report
            report_dir = os.path.dirname(batch1_report)
            comparison_file = os.path.join(report_dir, f"comparison_{batch1['batch_id']}_{batch2['batch_id']}.json")
            
            with open(comparison_file, 'w') as f:
                json.dump(comparison, f, indent=2)
                
            print(f"Comparison saved to {comparison_file}")
            print("\nSummary:")
            for line in summary:
                print(f"- {line}")
                
            return comparison
            
        except Exception as e:
            print(f"Error comparing batches: {e}")
            return None
    
    def analyze_source(self, source_path):
        """Analyze a source without processing it
        
        Args:
            source_path: Path to source (archive or HuggingFace dataset name)
            
        Returns:
            Dictionary with source analysis
        """
        try:
            # Create a temporary directory for extraction
            temp_extract_dir = os.path.join(self.temp_dir, "source_analysis")
            os.makedirs(temp_extract_dir, exist_ok=True)
            
            # Extract a sample of files (up to 100)
            print(f"Analyzing source: {source_path}")
            extracted_files = self.extractor.process(
                source_path, 
                temp_extract_dir, 
                max_files=100, 
                extract_mode="sample"
            )
            
            if not extracted_files:
                return {"error": "No files could be extracted from the source"}
                
            # Analyze the extracted files
            print(f"Analyzing {len(extracted_files)} sample files...")
            analysis_results = self.analyzer.process(extracted_files)
            
            # Filter the results
            filter_results = self.filter.process(analysis_results)
            
            # Calculate statistics
            stats = {
                "total_files": len(analysis_results),
                "acceptable_files": len(filter_results["accepted"]),
                "rejected_files": len(filter_results["rejected"]),
                "formats": {},
                "dimensions": {}
            }
            
            # Gather statistics about formats and dimensions
            for path, result in analysis_results.items():
                # Count formats
                format_name = result.get("format", "unknown")
                stats["formats"][format_name] = stats["formats"].get(format_name, 0) + 1
                
                # Track dimensions
                width = result.get("width", 0)
                height = result.get("height", 0)
                dim_key = f"{width}x{height}"
                stats["dimensions"][dim_key] = stats["dimensions"].get(dim_key, 0) + 1
            
            # Clean up temporary directory
            self.storage_manager.cleanup_directory(temp_extract_dir)
            
            return {
                "source": source_path,
                "sample_size": len(extracted_files),
                "statistics": stats
            }
            
        except Exception as e:
            return {
                "error": f"Error analyzing source: {str(e)}",
                "traceback": traceback.format_exc()
            }
    
    def filter_by_resolution(self, min_resolution=800):
        """Set minimum resolution filter
        
        Args:
            min_resolution: Minimum pixel dimension
            
        Returns:
            Self for method chaining
        """
        self.config["min_resolution"] = min_resolution
        return self
    
    def set_output_quality(self, quality=90):
        """Set output image quality
        
        Args:
            quality: Quality setting (1-100)
            
        Returns:
            Self for method chaining
        """
        self.config["quality"] = quality
        return self
    
    def set_output_format(self, format_name="webp"):
        """Set output format
        
        Args:
            format_name: Format to use (webp, jpg, png, etc.)
            
        Returns:
            Self for method chaining
        """
        self.config["output_format"] = format_name.lower()
        return self
    
    def enable_parallel_processing(self, enabled=True, max_workers=None):
        """Enable or disable parallel processing
        
        Args:
            enabled: Whether to enable parallel processing
            max_workers: Maximum number of worker threads
            
        Returns:
            Self for method chaining
        """
        self.config["parallel_processing"] = enabled
        if max_workers:
            self.config["max_workers"] = max_workers
        return self
    
    def enable_checkpointing(self, enabled=True, interval_minutes=10):
        """Enable or disable checkpointing
        
        Args:
            enabled: Whether to enable checkpointing
            interval_minutes: Checkpoint interval in minutes
            
        Returns:
            Self for method chaining
        """
        self.config["use_checkpointing"] = enabled
        self.config["checkpoint_interval"] = interval_minutes
        return self
