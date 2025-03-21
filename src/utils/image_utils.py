# image_utils.py - Image handling utilities

import os
from PIL import Image, ImageFile, ExifTags
import piexif
import logging

# Configure ImageFile to load truncated images
ImageFile.LOAD_TRUNCATED_IMAGES = True

def get_image_info(image_path):
    """Get detailed information about an image file
    
    Args:
        image_path: Path to the image file
        
    Returns:
        Dictionary containing image metadata (dimensions, format, etc.)
    """
    try:
        with Image.open(image_path) as img:
            # Extract EXIF data if available
            exif_data = {}
            try:
                if hasattr(img, '_getexif') and img._getexif():
                    exif = img._getexif()
                    if exif:
                        for tag, value in exif.items():
                            if tag in ExifTags.TAGS:
                                exif_data[ExifTags.TAGS[tag]] = value
            except Exception as e:
                pass  # Silently ignore EXIF errors
                
            return {
                "width": img.width,
                "height": img.height,
                "format": img.format,
                "mode": img.mode,
                "file_size": os.path.getsize(image_path),
                "path": image_path,
                "aspect_ratio": round(img.width / img.height, 3) if img.height > 0 else 0,
                "exif": exif_data
            }
    except Exception as e:
        return {
            "error": str(e),
            "path": image_path
        }

def convert_image(image_path, output_path, format="webp", quality=90, preserve_metadata=True, 
                 resize_if_larger=False, max_dimensions=(3840, 2160)):
    """Convert an image to a different format with specified quality
    
    Args:
        image_path: Path to the source image
        output_path: Path for the converted image
        format: Target format (default: webp)
        quality: Output quality 1-100 (default: 90)
        preserve_metadata: Whether to preserve image metadata
        resize_if_larger: Whether to resize images larger than max_dimensions
        max_dimensions: Maximum dimensions (width, height) for resizing
        
    Returns:
        Path to the converted image or None if conversion failed
    """
    try:
        with Image.open(image_path) as img:
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Get original format
            original_format = img.format
            
            # Extract metadata if needed
            exif_bytes = None
            if preserve_metadata and original_format in ['JPEG', 'TIFF']:
                try:
                    exif_dict = piexif.load(image_path)
                    exif_bytes = piexif.dump(exif_dict)
                except Exception as e:
                    print(f"Warning: Could not extract EXIF from {image_path}: {e}")
            
            # Resize if necessary
            if resize_if_larger and (img.width > max_dimensions[0] or img.height > max_dimensions[1]):
                img.thumbnail(max_dimensions, Image.LANCZOS)
            
            # Format-specific settings
            save_kwargs = {}
            if format.lower() in ['jpg', 'jpeg']:
                format = 'JPEG'
                save_kwargs = {'quality': quality, 'optimize': True}
                if exif_bytes:
                    save_kwargs['exif'] = exif_bytes
            elif format.lower() == 'webp':
                format = 'WEBP'
                save_kwargs = {'quality': quality, 'method': 6}  # Higher method = better compression but slower
                # WebP doesn't support EXIF directly in PIL, would need additional handling
            elif format.lower() == 'png':
                format = 'PNG'
                save_kwargs = {'optimize': True}
                # PNG compression level is 0-9, convert from 0-100 scale
                compress_level = min(9, int(9 * (1 - quality/100))) 
                save_kwargs['compress_level'] = compress_level
            
            # Save the image
            if img.mode == 'RGBA' and format == 'JPEG':
                # JPEG doesn't support alpha channel, convert to RGB
                img = img.convert('RGB')
                
            img.save(output_path, format=format, **save_kwargs)
            
            return output_path
    except Exception as e:
        print(f"Error converting {image_path}: {e}")
        return None

def is_valid_image(image_path, timeout=5):
    """Check if an image can be opened successfully with PIL
    
    Args:
        image_path: Path to the image file
        timeout: Maximum time in seconds to try opening the image
        
    Returns:
        Boolean indicating if the image is valid
    """
    try:
        # Use a timeout to avoid hanging on problematic files
        import signal
        
        # Define timeout handler
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Image validation timed out after {timeout} seconds")
            
        # Set timeout
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout)
        
        # Try to open and load the image
        with Image.open(image_path) as img:
            img.verify()  # Verify instead of load() for faster checking
            
        # Cancel timeout
        signal.alarm(0)
        return True
    except TimeoutError as e:
        logging.warning(f"Timeout while validating image {image_path}: {e}")
        return False
    except Exception as e:
        logging.debug(f"Invalid image {image_path}: {e}")
        return False

def resize_image(image_path, output_path, max_width=None, max_height=None, preserve_aspect=True):
    """Resize an image to specified dimensions
    
    Args:
        image_path: Path to the source image
        output_path: Path for the resized image
        max_width: Maximum width
        max_height: Maximum height
        preserve_aspect: Whether to preserve aspect ratio
        
    Returns:
        Path to the resized image or None if failed
    """
    try:
        with Image.open(image_path) as img:
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            if preserve_aspect:
                # Use thumbnail to preserve aspect ratio
                size = (max_width if max_width else img.width, max_height if max_height else img.height)
                img.thumbnail(size, Image.LANCZOS)
                img.save(output_path)
            else:
                # Resize to exact dimensions
                width = max_width if max_width else img.width
                height = max_height if max_height else img.height
                resized = img.resize((width, height), Image.LANCZOS)
                resized.save(output_path)
                
            return output_path
    except Exception as e:
        print(f"Error resizing {image_path}: {e}")
        return None

def calculate_average_color(image_path):
    """Calculate the average color of an image
    
    Args:
        image_path: Path to the image
        
    Returns:
        Tuple of (R, G, B) values or None if failed
    """
    try:
        with Image.open(image_path) as img:
            # Convert to RGB if needed
            if img.mode != 'RGB':
                img = img.convert('RGB')
                
            # Resize to 1x1 to get average color
            img = img.resize((1, 1), Image.LANCZOS)
            color = img.getpixel((0, 0))
            
            return color
    except Exception as e:
        print(f"Error calculating average color for {image_path}: {e}")
        return None
