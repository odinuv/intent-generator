import csv
import os
import logging
from datetime import datetime
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CSVSnowflakeClient:
    """
    A modified SnowflakeClient that reads from CSV files instead of querying Snowflake.
    Used for testing the intent analysis script with sample data.
    """
    
    def __init__(self, data_dir: str = "sample_data"):
        self.data_dir = data_dir
        logger.info("Initialized CSV-based Snowflake client")

    def get_configuration_versions(self, token_id: str, project_id: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        return self._read_csv_file(
            "kbc_component_configuration_version.csv",
            token_id, project_id, start_date, end_date,
            date_column="configuration_updated_at"
        )

    def get_configuration_row_versions(self, token_id: str, project_id: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        return self._read_csv_file(
            "kbc_component_configuration_row_version.csv",
            token_id, project_id, start_date, end_date,
            date_column="configuration_row_updated_at"
        )

    def get_jobs(self, token_id: str, project_id: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        return self._read_csv_file(
            "kbc_job.csv",
            token_id, project_id, start_date, end_date,
            date_column="job_created_at"
        )

    def get_table_events(self, token_id: str, project_id: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        return self._read_csv_file(
            "kbc_table_event.csv",
            token_id, project_id, start_date, end_date,
            date_column="event_created_at"
        )

    def get_distinct_project_ids(self, project_id_filter: str) -> List[str]:
        """Get distinct project IDs that match the filter"""
        file_path = os.path.join(self.data_dir, "kbc_component_configuration_version.csv")
        project_ids = set()
        
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if project_id_filter in row['kbc_project_id']:
                    project_ids.add(row['kbc_project_id'])
        
        return sorted(list(project_ids))

    def get_distinct_token_ids(self, project_id_filter: str) -> List[str]:
        """Get distinct token IDs for projects matching the filter"""
        file_path = os.path.join(self.data_dir, "kbc_component_configuration_version.csv")
        token_ids = set()
        
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if project_id_filter in row['kbc_project_id']:
                    token_ids.add(row['kbc_token_id'])
        
        return sorted(list(token_ids))

    def _read_csv_file(self, filename: str, token_id: str, project_id: str, 
                      start_date: datetime, end_date: datetime, date_column: str) -> List[Dict[str, Any]]:
        """Read and filter data from a CSV file"""
        file_path = os.path.join(self.data_dir, filename)
        results = []
        
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Filter by token_id and project_id
                if (row['kbc_token_id'] == token_id and 
                    row['kbc_project_id'] == project_id):
                    
                    # Filter by date range
                    row_date = datetime.fromisoformat(row[date_column].replace('Z', '+00:00'))
                    if start_date <= row_date <= end_date:
                        results.append(row)
        
        return results

    def close(self):
        """No connection to close for CSV client"""
        logger.info("CSV Snowflake client closed")


# Usage example:
# Replace the regular SnowflakeClient import in main.py with:
# from sample_data.csv_snowflake_client import CSVSnowflakeClient as SnowflakeClient 