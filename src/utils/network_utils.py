# network_utils.py - Network utilities for handling downloads and retries

import os
import time
import logging
import requests
from pathlib import Path
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

class NetworkManager:
    """Manages network operations with retry logic and error handling"""
    
    def __init__(self, max_retries=3, backoff_factor=0.5, 
                 status_forcelist=(500, 502, 503, 504, 429)):
        self.logger = logging.getLogger(__name__)
        
        # Configure retry strategy
        self.retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=["GET", "POST"]
        )
        
        # Create a session with the retry adapter
        self.session = requests.Session()
        adapter = HTTPAdapter(max_retries=self.retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def download_file(self, url, output_path, chunk_size=8192, timeout=30, 
                      progress_callback=None):
        """Download a file with retry logic
        
        Args:
            url: URL to download
            output_path: Path to save the file
            chunk_size: Size of chunks to download
            timeout: Request timeout in seconds
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Start the request with stream=True for large files
            response = self.session.get(url, stream=True, timeout=timeout)
            response.raise_for_status()
            
            # Get file size if available
            total_size = int(response.headers.get('content-length', 0))
            
            # Download in chunks to handle large files
            downloaded_size = 0
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:  # filter out keep-alive chunks
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Update progress if callback provided
                        if progress_callback and total_size > 0:
                            progress = (downloaded_size / total_size) * 100
                            progress_callback(progress)
            
            # Verify file was downloaded
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                self.logger.info(f"Downloaded {url} to {output_path}")
                return output_path
            else:
                self.logger.error(f"Downloaded file {output_path} is empty")
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error downloading {url}: {e}")
            
            # Handle specific errors
            if isinstance(e, requests.exceptions.ConnectTimeout):
                self.logger.warning("Connection timed out. Check your internet connection.")
            elif isinstance(e, requests.exceptions.ConnectionError):
                self.logger.warning("Connection error. Server might be down or unreachable.")
            elif isinstance(e, requests.exceptions.TooManyRedirects):
                self.logger.warning("Too many redirects. URL might be incorrect.")
            elif isinstance(e, requests.exceptions.HTTPError):
                status_code = e.response.status_code
                if status_code == 404:
                    self.logger.warning(f"Resource not found (404): {url}")
                elif status_code == 403:
                    self.logger.warning(f"Access forbidden (403): {url}")
                elif status_code == 429:
                    self.logger.warning(f"Rate limited (429). Waiting before retry.")
                    time.sleep(10)  # Wait longer for rate limiting
            
            # Clean up partial download
            if os.path.exists(output_path):
                os.remove(output_path)
                
            return None
            
        except Exception as e:
            self.logger.error(f"Unexpected error downloading {url}: {e}")
            
            # Clean up partial download
            if os.path.exists(output_path):
                os.remove(output_path)
                
            return None
    
    def check_url_exists(self, url, timeout=10):
        """Check if a URL exists and is accessible
        
        Args:
            url: URL to check
            timeout: Request timeout in seconds
            
        Returns:
            Boolean indicating if URL exists and is accessible
        """
        try:
            # Use HEAD request to minimize data transfer
            response = self.session.head(url, timeout=timeout)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
            
    def close(self):
        """Close the session"""
        self.session.close()
