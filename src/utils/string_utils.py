# string_utils.py - String manipulation utilities

import os
import re
import random
import string

def generate_unique_id(prefix="batch", length=8):
    """Generate a unique ID with a prefix
    
    Args:
        prefix: String prefix for the ID
        length: Length of the random part
        
    Returns:
        String unique ID
    """
    random_part = ''.join(random.choices(string.digits, k=length))
    return f"{prefix}{random_part}"

def sanitize_filename(filename):
    """Sanitize a filename by removing invalid characters
    
    Args:
        filename: Original filename
        
    Returns:
        Sanitized filename
    """
    # Replace invalid filename characters with underscore
    sanitized = re.sub(r'[\\/*?:"<>|]', "_", filename)
    return sanitized

def get_extension(filename):
    """Get the file extension from a filename
    
    Args:
        filename: Filename to extract extension from
        
    Returns:
        File extension (lowercase, without the dot)
    """
    _, ext = os.path.splitext(filename)
    return ext.lower()[1:] if ext else ""

def is_image_file(filename, valid_extensions=None):
    """Check if a filename has an image extension
    
    Args:
        filename: Filename to check
        valid_extensions: List of valid image extensions (default: common image formats)
        
    Returns:
        Boolean indicating if the file has an image extension
    """
    if valid_extensions is None:
        valid_extensions = ["jpg", "jpeg", "png", "gif", "bmp", "webp", "tiff"]
        
    ext = get_extension(filename)
    return ext.lower() in valid_extensions
