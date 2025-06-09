import os
import logging
from datetime import datetime
from typing import List, Dict, Any
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SnowflakeClient:
    def __init__(self):
        logger.info("Initializing Snowflake connection...")
        self.conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        logger.info("Snowflake connection established successfully")

    def get_configuration_versions(self, token_id: str, project_id: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        query = """
        SELECT 
            "kbc_component_configuration_id",
            "configuration_updated_at",
            "configuration_version",
            "configuration_json",
            "change_description_short"
        FROM "kbc_component_configuration_version"
        WHERE "kbc_token_id" = %s
        AND "kbc_project_id" = %s
        AND "configuration_updated_at" >= %s
        AND "configuration_updated_at" <= %s
        ORDER BY "configuration_updated_at"
        """
        #logger.info(f"Executing configuration versions query for token_id={token_id}, project_id={project_id}")
        return self._execute_query(query, (token_id, project_id, start_date.strftime('%Y-%m-%dT%H:%M:%S%z'), end_date.strftime('%Y-%m-%dT%H:%M:%S%z')))

    def get_configuration_row_versions(self, token_id: str, project_id: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        query = """
        SELECT 
            "kbc_component_configuration_row_id",
            "kbc_component_configuration_id",
            "configuration_row_updated_at",
            "configuration_row_version",
            "configuration_row_json",
            "change_description_short"
        FROM "kbc_component_configuration_row_version"
        WHERE "kbc_token_id" = %s
        AND "kbc_project_id" = %s
        AND "configuration_row_updated_at" >= %s
        AND "configuration_row_updated_at" <= %s
        ORDER BY "configuration_row_updated_at"
        """
        #logger.info(f"Executing configuration row versions query for token_id={token_id}, project_id={project_id}")
        return self._execute_query(query, (token_id, project_id, start_date.strftime('%Y-%m-%dT%H:%M:%S%z'), end_date.strftime('%Y-%m-%dT%H:%M:%S%z')))

    def get_jobs(self, token_id: str, project_id: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        query = """
        SELECT 
            "kbc_job_id",
            "kbc_component_configuration_id",
            "job_start_at",
            "job_created_at",
            "job_status",
            "error_type",
            "error_message",
            "error_message_short"
        FROM "kbc_job"
        WHERE "kbc_token_id" = %s
        AND "kbc_project_id" = %s
        AND "job_created_at" >= %s
        AND "job_created_at" <= %s
        ORDER BY "job_created_at"
        """
        #logger.info(f"Executing jobs query for token_id={token_id}, project_id={project_id}")
        return self._execute_query(query, (token_id, project_id, start_date.strftime('%Y-%m-%dT%H:%M:%S%z'), end_date.strftime('%Y-%m-%dT%H:%M:%S%z')))

    def get_table_events(self, token_id: str, project_id: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        query = """
        SELECT 
            "kbc_table_event_id",
            "table_id",
            "event_created_at",
            "event",
            "event_type",
            "message",
            "params"
        FROM "kbc_table_event"
        WHERE "kbc_token_id" = %s
        AND "kbc_project_id" = %s
        AND "event_created_at" >= %s
        AND "event_created_at" <= %s
        ORDER BY "event_created_at"
        """
        #logger.info(f"Executing table events query for token_id={token_id}, project_id={project_id}")
        return self._execute_query(query, (token_id, project_id, start_date.strftime('%Y-%m-%dT%H:%M:%S%z'), end_date.strftime('%Y-%m-%dT%H:%M:%S%z')))

    def get_distinct_project_ids(self, project_id_filter: str) -> List[str]:
        query = """
        SELECT DISTINCT "kbc_project_id"
        FROM "kbc_component_configuration_version"
        WHERE "kbc_project_id" LIKE %s
        ORDER BY "kbc_project_id"
        """
        #logger.info(f"Executing distinct project IDs query with filter={project_id_filter}")
        results = self._execute_query(query, (f'%{project_id_filter}',))
        return [row['kbc_project_id'] for row in results]

    def get_distinct_token_ids(self, project_id_filter: str) -> List[str]:
        query = """
        SELECT DISTINCT "kbc_token_id"
        FROM "kbc_component_configuration_version"
        WHERE "kbc_project_id" LIKE %s
        ORDER BY "kbc_token_id"
        """
        #logger.info(f"Executing distinct token IDs query with project filter={project_id_filter}")
        results = self._execute_query(query, (f'%{project_id_filter}',))
        return [row['kbc_token_id'] for row in results]

    def _execute_query(self, query: str, params: tuple) -> List[Dict[str, Any]]:
        #logger.info(f"Executing query: {query}")
        #logger.info(f"With parameters: {params}")
        cursor = self.conn.cursor(snowflake.connector.DictCursor)
        try:
            cursor.execute(query, params)
            results = cursor.fetchall()
            # logger.info(f"Query executed successfully, returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
        finally:
            cursor.close()

    def close(self):
        logger.info("Closing Snowflake connection...")
        self.conn.close()
        logger.info("Snowflake connection closed") 