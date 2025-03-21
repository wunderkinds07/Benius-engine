# report_utils.py - Reporting utilities

import csv
import os
import json
import datetime

class ReportGenerator:
    """Generates reports on image processing results"""
    
    def __init__(self, config):
        self.config = config
        self.report_dir = config.get("report_directory", "output/reports")
        os.makedirs(self.report_dir, exist_ok=True)
    
    def generate_csv_report(self, data, report_name="report"):
        """Generate a CSV report from the provided data
        
        Args:
            data: List of dictionaries with consistent keys
            report_name: Base name for the report file
            
        Returns:
            Path to the generated report
        """
        if not data or not isinstance(data, list) or len(data) == 0:
            return None
            
        # Create a timestamp for the report filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{report_name}_{timestamp}.csv"
        filepath = os.path.join(self.report_dir, filename)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # Write the CSV file
        with open(filepath, 'w', newline='') as csvfile:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for row in data:
                writer.writerow(row)
                
        return filepath
    
    def generate_summary_report(self, batch_results, output_path=None):
        """Generate a summary report of batch processing
        
        Args:
            batch_results: Dictionary with batch processing results
            output_path: Optional specific path for the report
            
        Returns:
            Path to the generated report
        """
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if not output_path:
            filename = f"batch_summary_{timestamp}.json"
            output_path = os.path.join(self.report_dir, filename)
            
        # Ensure the directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Add timestamp to the results
        summary = batch_results.copy()
        summary["generated_at"] = timestamp
        
        # Write the summary to a JSON file
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)
            
        return output_path
    
    def generate_rejected_files_report(self, rejected_files, reason_key="reason"):
        """Generate a report of rejected files
        
        Args:
            rejected_files: Dictionary mapping file paths to rejection information
            reason_key: Key in the rejection info containing the reason
            
        Returns:
            Path to the generated report
        """
        report_data = []
        
        for file_path, info in rejected_files.items():
            row = {
                "file_path": file_path,
                "reason": info.get(reason_key, "Unknown reason") 
            }
            
            # Add all other info keys
            for key, value in info.items():
                if key != reason_key:
                    row[key] = value
                    
            report_data.append(row)
            
        return self.generate_csv_report(report_data, report_name="rejected_files")
