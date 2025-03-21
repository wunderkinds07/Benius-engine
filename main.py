#!/usr/bin/env python3
# main.py - Main entry point for Image Processing Engine

import os
import sys
import json
import argparse
import glob
from pathlib import Path
from src.batch_processor import BatchProcessor

def load_config(config_path="config/config.json"):
    """Load configuration from file
    
    Args:
        config_path: Path to the config file
        
    Returns:
        Dictionary with configuration values
    """
    # Default configuration
    default_config = {
        "min_resolution": 800,
        "output_format": "webp",
        "quality": 90,
        "batch_prefix": "batch",
        "parallel_processing": True,
        "max_workers": 8,
        "enable_checkpoints": True,
        "database_path": "database/images.db",
        "huggingface_cache": "data/huggingface_cache",
        "delete_after_packaging": True,
        "show_progress": True,
        "resize_if_larger": False,
        "max_dimensions": (3840, 2160)
    }
    
    # Try to load config from file
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                file_config = json.load(f)
                # Update default config with file values
                default_config.update(file_config)
    except Exception as e:
        print(f"Warning: Could not load config from {config_path}: {e}")
        print("Using default configuration")
    
    return default_config

def save_config(config, config_path="config/config.json"):
    """Save configuration to file
    
    Args:
        config: Configuration dictionary
        config_path: Path to save the config file
        
    Returns:
        Boolean indicating success
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving config to {config_path}: {e}")
        return False

def find_archives(directory="."):
    """Find archive files in the specified directory
    
    Args:
        directory: Directory to search in
        
    Returns:
        List of archive file paths
    """
    archives = []
    for ext in ["*.zip", "*.tar", "*.tar.gz", "*.tgz"]:
        archives.extend(glob.glob(os.path.join(directory, ext)))
    return archives

def interactive_mode():
    """Run the application in interactive mode
    
    Returns:
        Status code (0 for success)
    """
    print("-" * 60)
    print("Image Processing Engine - Interactive Mode")
    print("-" * 60)
    
    # Load default configuration
    config = load_config()
    
    # Select source
    print("\nSource Selection:")
    print("1. Local archive file (ZIP/TAR)")
    print("2. HuggingFace dataset")
    print("3. Directory of images")
    
    while True:
        try:
            source_type = int(input("\nSelect source type [1-3]: "))
            if 1 <= source_type <= 3:
                break
            else:
                print("Invalid option. Please enter a number between 1 and 3.")
        except ValueError:
            print("Please enter a valid number.")
    
    source_path = ""
    
    if source_type == 1:
        # Find available archives
        print("\nSearching for archive files...")
        search_dir = input("Enter directory to search for archives [./]: ").strip() or "."
        archives = find_archives(search_dir)
        
        if not archives:
            print("No archive files found.")
            custom_path = input("Enter full path to archive file: ").strip()
            if os.path.exists(custom_path):
                source_path = custom_path
            else:
                print(f"Error: File {custom_path} not found.")
                return 1
        else:
            print("\nAvailable archives:")
            for i, archive in enumerate(archives, 1):
                print(f"{i}. {archive}")
            print(f"{len(archives) + 1}. Enter custom path")
            
            while True:
                try:
                    selection = int(input(f"\nSelect archive [1-{len(archives) + 1}]: "))
                    if 1 <= selection <= len(archives):
                        source_path = archives[selection - 1]
                        break
                    elif selection == len(archives) + 1:
                        custom_path = input("Enter full path to archive file: ").strip()
                        if os.path.exists(custom_path):
                            source_path = custom_path
                            break
                        else:
                            print(f"Error: File {custom_path} not found.")
                    else:
                        print(f"Invalid option. Please enter a number between 1 and {len(archives) + 1}.")
                except ValueError:
                    print("Please enter a valid number.")
    
    elif source_type == 2:  # HuggingFace dataset
        source_path = input("Enter HuggingFace dataset name: ").strip()
    
    elif source_type == 3:  # Directory of images
        while True:
            dir_path = input("Enter directory path containing images: ").strip()
            if os.path.isdir(dir_path):
                source_path = dir_path
                break
            else:
                print(f"Error: Directory {dir_path} not found.")
    
    if not source_path:
        print("Error: No source selected.")
        return 1
    
    # Configure processing options
    print("\n" + "-" * 60)
    print("Processing Options:")
    print("-" * 60)
    
    print("\nCurrent configuration:")
    for key, value in config.items():
        print(f"{key}: {value}")
    
    if input("\nWould you like to modify these settings? (y/n): ").lower().startswith('y'):
        # Resolution setting
        while True:
            try:
                res = input(f"Minimum resolution (pixels) [{config['min_resolution']}]: ").strip()
                if res:
                    config["min_resolution"] = int(res)
                break
            except ValueError:
                print("Please enter a valid number.")
        
        # Quality setting
        while True:
            try:
                quality = input(f"Output quality (1-100) [{config['quality']}]: ").strip()
                if quality:
                    quality_val = int(quality)
                    if 1 <= quality_val <= 100:
                        config["quality"] = quality_val
                        break
                    else:
                        print("Quality must be between 1 and 100.")
                else:
                    break
            except ValueError:
                print("Please enter a valid number.")
        
        # Add resize option
        resize_option = input(f"Enable resizing for large images (y/n) [{'y' if config.get('resize_if_larger', False) else 'n'}]: ").strip().lower()
        if resize_option:
            config["resize_if_larger"] = resize_option.startswith('y')
        
        if config["resize_if_larger"]:
            # Ask for max dimensions
            current_max = config.get("max_dimensions", (3840, 2160))
            while True:
                try:
                    max_width = input(f"Maximum width in pixels [{current_max[0]}]: ").strip()
                    if max_width:
                        max_width = int(max_width)
                        if max_width > 0:
                            break
                        else:
                            print("Width must be greater than 0.")
                    else:
                        max_width = current_max[0]
                        break
                except ValueError:
                    print("Please enter a valid number.")
            
            while True:
                try:
                    max_height = input(f"Maximum height in pixels [{current_max[1]}]: ").strip()
                    if max_height:
                        max_height = int(max_height)
                        if max_height > 0:
                            break
                        else:
                            print("Height must be greater than 0.")
                    else:
                        max_height = current_max[1]
                        break
                except ValueError:
                    print("Please enter a valid number.")
            
            config["max_dimensions"] = (max_width, max_height)
        
        # Format setting
        format_options = ["webp", "jpeg", "png"]
        print("\nOutput Format Options:")
        for i, fmt in enumerate(format_options, 1):
            print(f"{i}. {fmt}")
        
        while True:
            try:
                fmt = input(f"Select output format [1-{len(format_options)}] (default: {format_options.index(config['output_format']) + 1}): ").strip()
                if fmt:
                    fmt_idx = int(fmt) - 1
                    if 0 <= fmt_idx < len(format_options):
                        config["output_format"] = format_options[fmt_idx]
                        break
                    else:
                        print(f"Please enter a number between 1 and {len(format_options)}.")
                else:
                    break
            except ValueError:
                print("Please enter a valid number.")
        
        # Parallel processing
        parallel = input(f"Enable parallel processing (y/n) [{'y' if config['parallel_processing'] else 'n'}]: ").strip().lower()
        if parallel:
            config["parallel_processing"] = parallel.startswith('y')
        
        if config["parallel_processing"]:
            while True:
                try:
                    workers = input(f"Number of workers [{config['max_workers']}]: ").strip()
                    if workers:
                        config["max_workers"] = int(workers)
                    break
                except ValueError:
                    print("Please enter a valid number.")
        
        # Output directory
        output_dir = input(f"Output directory [{config.get('output_directory', 'output')}]: ").strip()
        if output_dir:
            config["output_directory"] = output_dir
        
        # Save updated configuration
        if input("\nSave these settings for future use? (y/n): ").lower().startswith('y'):
            save_config(config)
            print("Configuration saved.")
    
    # Confirm and process
    print("\n" + "-" * 60)
    print("Ready to Process:")
    print("-" * 60)
    print(f"Source: {source_path}")
    print(f"Min Resolution: {config['min_resolution']}px")
    print(f"Output Format: {config['output_format']} (Quality: {config['quality']}%)")
    if config["resize_if_larger"]:
        print(f"Resizing: Enabled (Max Dimensions: {config['max_dimensions'][0]}x{config['max_dimensions'][1]})")
    else:
        print(f"Resizing: Disabled")
    print(f"Parallel Processing: {'Enabled' if config['parallel_processing'] else 'Disabled'}")
    if config['parallel_processing']:
        print(f"Workers: {config['max_workers']}")
    print(f"Output Directory: {config.get('output_directory', 'output')}")
    
    if not input("\nContinue with processing? (y/n): ").lower().startswith('y'):
        print("Processing cancelled.")
        return 0
    
    # Initialize processor and process source
    print("\nInitializing processor...")
    processor = BatchProcessor(config)
    
    analyze_only = input("\nOnly analyze the source without processing? (y/n): ").lower().startswith('y')
    
    print("\nProcessing source...")
    try:
        if analyze_only:
            result = processor.analyze_source(source_path)
        else:
            result = processor.execute(source_path)
        print("\nProcessing complete!")
        return 0
    except Exception as e:
        print(f"\nError during processing: {e}")
        return 1

def main():
    parser = argparse.ArgumentParser(description="Image Processing Engine")
    parser.add_argument("source", nargs="?", help="Source path (archive file or HuggingFace dataset)")
    parser.add_argument("-c", "--config", default="config/config.json", help="Path to config file")
    parser.add_argument("-r", "--min-resolution", type=int, help="Minimum resolution in pixels")
    parser.add_argument("-q", "--quality", type=int, help="Output quality (1-100)")
    parser.add_argument("-f", "--format", help="Output format (webp, jpg, png)")
    parser.add_argument("-o", "--output-dir", help="Output directory")
    parser.add_argument("--analyze-only", action="store_true", help="Only analyze source without processing")
    parser.add_argument("-i", "--interactive", action="store_true", help="Run in interactive mode")
    parser.add_argument("--resize-if-larger", action="store_true", help="Enable resizing for large images")
    parser.add_argument("--max-width", type=int, help="Maximum width in pixels")
    parser.add_argument("--max-height", type=int, help="Maximum height in pixels")
    parser.add_argument("--project-dir", help="Project root directory for all files and outputs")
    parser.add_argument("--sample", action="store_true", help="Enable sample mode to process fewer images")
    parser.add_argument("--sample-size", type=int, default=100, help="Number of images to process in sample mode")
    parser.add_argument("--sample-random", action="store_true", help="Select random samples instead of first N")
    
    args = parser.parse_args()
    
    # Run in interactive mode if specified or if no source is provided
    if args.interactive or (len(sys.argv) == 1):
        return interactive_mode()
    
    # Load configuration
    config = load_config(args.config)
    
    # Override config with command-line arguments
    if args.min_resolution:
        config["min_resolution"] = args.min_resolution
    if args.quality:
        config["quality"] = args.quality
    if args.format:
        config["output_format"] = args.format
    if args.output_dir:
        config["output_directory"] = args.output_dir
    if args.resize_if_larger:
        config["resize_if_larger"] = True
    if args.max_width:
        config["max_dimensions"] = (args.max_width, config["max_dimensions"][1])
    if args.max_height:
        config["max_dimensions"] = (config["max_dimensions"][0], args.max_height)
        
    # Handle project directory setting
    if args.project_dir:
        project_dir = os.path.abspath(args.project_dir)
        config["project_root"] = project_dir
        
        # Update all paths to be within project directory
        print(f"Using project directory: {project_dir}")
        os.makedirs(project_dir, exist_ok=True)
        
        # Update all directory paths to be within project directory
        for dir_key in ["temp_directory", "output_directory", "report_directory", "database_path", "checkpoint_directory"]:
            if dir_key in config:
                # For database_path, handle differently as it's a file
                if dir_key == "database_path":
                    db_dir = os.path.dirname(os.path.join(project_dir, config[dir_key]))
                    os.makedirs(db_dir, exist_ok=True)
                    config[dir_key] = os.path.join(project_dir, config[dir_key])
                else:
                    # Make path absolute within project directory
                    path = os.path.join(project_dir, config[dir_key])
                    os.makedirs(path, exist_ok=True)
                    config[dir_key] = path
    
    # Handle sample mode
    if args.sample:
        config["sample_mode"] = True
        config["sample_size"] = args.sample_size
        config["sample_random"] = args.sample_random
        print(f"Sample mode enabled: Processing {args.sample_size} images {'randomly' if args.sample_random else 'sequentially'}")
    
    # Initialize batch processor
    processor = BatchProcessor(config)
    
    if not args.source:
        print("Error: Source path is required")
        parser.print_help()
        return 1
    
    # Process or analyze source
    try:
        if args.analyze_only:
            result = processor.analyze_source(args.source)
        else:
            result = processor.execute(args.source)
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
