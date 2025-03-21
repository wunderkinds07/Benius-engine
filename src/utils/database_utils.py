# database_utils.py - Database utilities

import sqlite3
import os
import json

class DatabaseManager:
    """Manages database operations for image processing tracking"""
    
    def __init__(self, config):
        self.config = config
        self.db_path = config.get("database_path", "database/images.db")
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.init_database()
    
    def init_database(self):
        """Initialize the database with necessary tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables for batches and images
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            id INTEGER PRIMARY KEY,
            batch_id TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source TEXT,
            status TEXT,
            metadata TEXT
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS images (
            id INTEGER PRIMARY KEY,
            batch_id TEXT,
            original_path TEXT,
            processed_path TEXT,
            width INTEGER,
            height INTEGER,
            format TEXT,
            status TEXT,
            metadata TEXT,
            FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
        )
        """)
        
        conn.commit()
        conn.close()
    
    def add_batch(self, batch_id, source, metadata=None):
        """Add a new batch to the database
        
        Args:
            batch_id: Unique identifier for the batch
            source: Source of the images
            metadata: Additional batch information
            
        Returns:
            ID of the inserted batch
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        metadata_json = json.dumps(metadata) if metadata else None
        
        cursor.execute(
            "INSERT INTO batches (batch_id, source, status, metadata) VALUES (?, ?, ?, ?)",
            (batch_id, source, "created", metadata_json)
        )
        
        batch_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return batch_id
    
    def register_image(self, batch_id, original_path, image_data):
        """Register an image in the database
        
        Args:
            batch_id: ID of the batch this image belongs to
            original_path: Original path of the image
            image_data: Dictionary with image metadata
            
        Returns:
            ID of the inserted image record
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        width = image_data.get("width", 0)
        height = image_data.get("height", 0)
        format = image_data.get("format", "")
        processed_path = image_data.get("processed_path", "")
        
        # Remove keys that are stored in separate columns
        metadata = image_data.copy()
        for key in ["width", "height", "format", "processed_path"]:
            if key in metadata:
                del metadata[key]
        
        metadata_json = json.dumps(metadata)
        
        cursor.execute("""
        INSERT INTO images 
        (batch_id, original_path, processed_path, width, height, format, status, metadata) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (batch_id, original_path, processed_path, width, height, format, "registered", metadata_json))
        
        image_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return image_id
    
    def update_image(self, image_id, status, processed_path=None, metadata=None):
        """Update image information in the database
        
        Args:
            image_id: ID of the image to update
            status: New status for the image
            processed_path: Path to the processed image (if available)
            metadata: Additional metadata to store
            
        Returns:
            Boolean indicating success
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get existing metadata
        cursor.execute("SELECT metadata FROM images WHERE id = ?", (image_id,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            return False
            
        existing_metadata = json.loads(result[0]) if result[0] else {}
        
        # Update metadata if provided
        if metadata:
            existing_metadata.update(metadata)
        
        metadata_json = json.dumps(existing_metadata)
        
        # Update the record
        if processed_path:
            cursor.execute(
                "UPDATE images SET status = ?, processed_path = ?, metadata = ? WHERE id = ?",
                (status, processed_path, metadata_json, image_id)
            )
        else:
            cursor.execute(
                "UPDATE images SET status = ?, metadata = ? WHERE id = ?",
                (status, metadata_json, image_id)
            )
        
        conn.commit()
        conn.close()
        
        return True
