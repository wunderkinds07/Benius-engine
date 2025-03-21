# huggingface_utils.py - HuggingFace dataset utilities

import os
from datasets import load_dataset

class HuggingFaceManager:
    """Manages interactions with HuggingFace datasets"""
    
    def __init__(self, config):
        self.config = config
        self.cache_dir = config.get("huggingface_cache", "data/huggingface_cache")
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def load_dataset(self, dataset_name, split="train"):
        """Load a dataset from HuggingFace
        
        Args:
            dataset_name: Name of the dataset to load
            split: Dataset split to load (default: train)
            
        Returns:
            Loaded dataset object
        """
        try:
            dataset = load_dataset(dataset_name, split=split, cache_dir=self.cache_dir)
            return dataset
        except Exception as e:
            print(f"Error loading dataset {dataset_name}: {e}")
            return None
    
    def extract_images(self, dataset, image_column="image", output_dir=None):
        """Extract images from a HuggingFace dataset
        
        Args:
            dataset: HuggingFace dataset object
            image_column: Column name containing images
            output_dir: Directory to save extracted images
            
        Returns:
            List of paths to extracted images
        """
        if output_dir is None:
            output_dir = os.path.join("data", "extracted_images")
            
        os.makedirs(output_dir, exist_ok=True)
        image_paths = []
        
        # Process dataset and extract images
        for i, example in enumerate(dataset):
            if image_column in example:
                try:
                    # Access the image (PIL Image object)
                    image = example[image_column]
                    # Save the image to the output directory
                    image_path = os.path.join(output_dir, f"image_{i:06d}.png")
                    image.save(image_path)
                    image_paths.append(image_path)
                except Exception as e:
                    print(f"Error extracting image {i}: {e}")
            
        return image_paths
    
    def get_dataset_info(self, dataset_name):
        """Get information about a HuggingFace dataset
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            Dictionary with dataset information
        """
        try:
            # Load just the dataset info without downloading the full dataset
            info = load_dataset(dataset_name, split=None)
            return {
                "name": dataset_name,
                "splits": list(info.keys()),
                "features": {split: list(info[split].features.keys()) for split in info}
            }
        except Exception as e:
            return {"name": dataset_name, "error": str(e)}
