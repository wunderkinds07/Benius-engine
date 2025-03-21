# Image Processing Engine

An end-to-end system for batch processing, filtering, and converting images from various sources. This engine extracts images from archives or HuggingFace datasets, filters them based on criteria like resolution, converts them to optimized formats, and packages them for distribution.

## Features

- Extract images from TAR, ZIP archives, or HuggingFace datasets
- Batch renaming with sequential IDs
- Image analysis and filtering based on configurable criteria
- Conversion to optimized formats (WebP default) with quality settings
- Parallel processing for improved performance
- Progress tracking and reporting
- Database storage of image metadata and processing history
- Checkpointing to resume interrupted processing
- Comprehensive error handling and logging

## Setup

### Prerequisites

- Python 3.8+
- Required packages installed (see requirements.txt)

### Installation

1. Clone the repository or extract the project
2. Install the dependencies:

```bash
pip install -r requirements.txt
```

3. Create required directories:

```bash
mkdir -p config data output database checkpoints
```

## Usage

### Interactive Mode

For a guided experience, use interactive mode:

```bash
python main.py -i
# OR simply run without arguments
python main.py
```

This will provide a step-by-step interface to:
- Select your source (archive file, HuggingFace dataset, or directory)
- Configure processing options
- View processing results

### Basic Usage

Process an image archive:

```bash
python main.py path/to/your/archive.zip
```

Process a HuggingFace dataset:

```bash
python main.py dataset_name
```

Process a directory of images:

```bash
python main.py path/to/image/directory
```

### Command Line Options

```
python main.py [source] [options]
```

Options:
- `-i, --interactive`: Run in interactive mode
- `-c, --config`: Path to config file (default: config/config.json)
- `-r, --min-resolution`: Minimum resolution in pixels
- `-q, --quality`: Output quality (1-100)
- `-f, --format`: Output format (webp, jpg, png)
- `-o, --output-dir`: Output directory
- `--analyze-only`: Only analyze source without processing

### Configuration

The system can be configured via the `config/config.json` file. Key settings include:

- `min_resolution`: Minimum image dimensions (default: 800px)
- `output_format`: Target image format (default: webp)
- `quality`: Output quality 1-100 (default: 90)
- `parallel_processing`: Enable parallel processing (default: true)
- `max_workers`: Number of parallel workers (default: 8)
- `enable_checkpoints`: Enable processing checkpoints (default: true)
- `delete_after_packaging`: Remove temporary files after packaging (default: true)

## Example Workflow

1. **Prepare your source images**:
   - Collect images in a ZIP/TAR archive or use a HuggingFace dataset

2. **Process the images**:
   ```bash
   python main.py my_images.zip -r 1024 -q 85 -f webp -o processed_output
   ```

3. **Review the results**:
   - Check the generated ZIP file in the output directory
   - Review the JSON summary report for processing statistics
   - Look at the database for detailed image records

4. **Resume interrupted processing**:
   If processing was interrupted, simply run the same command again. The system will detect the checkpoint and resume from where it left off:
   ```bash
   python main.py my_images.zip -r 1024 -q 85 -f webp -o processed_output
   ```

## Project Structure

```
/
├─src/
│ ├─phases/           # Processing pipeline phases
│ │ ├─extractor.py    # Image extraction from sources
│ │ ├─renamer.py      # Batch renaming
│ │ ├─analyzer.py     # Image analysis
│ │ ├─filter.py       # Resolution/quality filtering
│ │ ├─converter.py    # Format conversion
│ │ └─packager.py     # Output packaging
│ ├─utils/            # Utility modules
│ │ ├─image_utils.py  # Image handling functions
│ │ ├─database_utils.py # Database operations
│ │ ├─report_utils.py # Report generation
│ │ ├─progress_utils.py # Progress tracking
│ │ ├─huggingface_utils.py # HuggingFace integration
│ │ └─string_utils.py # String manipulation utilities
│ └─batch_processor.py # Main orchestrator
├─config/             # Configuration files
├─data/               # Input and temporary data
├─output/             # Output files and reports
├─database/           # Database storage
├─checkpoints/        # Processing checkpoints
└─main.py             # Entry point
```

## Output

The system produces:

1. A ZIP archive containing all processed images
2. A JSON summary report with processing statistics
3. A CSV report of any rejected images
4. Database entries for all processed batches and images

## Troubleshooting

- **Processing hangs**: Try reducing the number of workers in the config file
- **Memory issues**: Process smaller batches or disable parallel processing
- **Corrupt images**: These will be automatically filtered and reported
- **Database errors**: Check permissions on the database directory

## License

Proprietary - All rights reserved
