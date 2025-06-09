import json
import os
import logging
from datetime import datetime
from src.session_analyzer import SessionAnalyzer
from src.snowflake_client import SnowflakeClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Initial test case
    # token_id = "155991_kbc-eu-central-1"
    # project_id_filter = "3082_kbc-eu-central-1"
    project_id_filter = "keboola-azure-north-europe"
    start_date = datetime(2024, 12, 1)
    end_date = datetime(2024, 12, 31)

    # Initialize clients
    snowflake_client = SnowflakeClient()
    analyzer = SessionAnalyzer()

    try:
        # Get all matching project IDs
        project_ids = snowflake_client.get_distinct_project_ids(project_id_filter)
        logger.info(f"Found {len(project_ids)} projects matching filter '{project_id_filter}'")

        # Create output directory and open files for writing
        os.makedirs('output', exist_ok=True)
        
        total_intents = 0
        total_errors = 0

        with open('output/intents.jsonl', 'w', buffering=1) as intents_file, \
             open('output/errors.jsonl', 'w', buffering=1) as errors_file:

            # Process each project
            for project_id in project_ids:
                logger.info(f"\nProcessing project: {project_id}")
                
                # Get all token IDs for this project
                token_ids = snowflake_client.get_distinct_token_ids(project_id)
                logger.info(f"Found {len(token_ids)} tokens in project {project_id}")

                # Process each token
                for token_id in token_ids:
                    logger.info(f"\nProcessing token: {token_id}")
                    try:
                        intents, errors = analyzer.analyze_user_sessions(token_id, project_id, start_date, end_date)
                        
                        # Write intents to file immediately
                        for intent in intents:
                            intent_data = {
                                'start_time': intent.start_time.isoformat(),
                                'end_time': intent.end_time.isoformat(),
                                'token_id': intent.token_id,
                                'project_id': intent.project_id,
                                'configuration_ids': intent.configuration_ids,
                                'intent_description': intent.intent_description,
                                'is_successful': intent.is_successful,
                                'session_id': intent.session_id,
                                'fulfillment': intent.fulfillment,
                                'tags': intent.tags,
                                'classification': intent.classification,
                                'development_stage': intent.development_stage,
                                'summary': intent.summary
                            }
                            intents_file.write(json.dumps(intent_data) + '\n')
                        intents_file.flush()  # Force Python buffer flush
                        logger.info(f"DEBUG: Wrote {len(intents)} intents to file")
                        
                        # Write errors to file immediately
                        for error in errors:
                            error_data = {
                                'start_time': error.start_time.isoformat(),
                                'end_time': error.end_time.isoformat(),
                                'token_id': error.token_id,
                                'project_id': error.project_id,
                                'configuration_ids': error.configuration_ids,
                                'error_category': error.error_category.value,
                                'context': error.context
                            }
                            errors_file.write(json.dumps(error_data) + '\n')
                        errors_file.flush()  # Force Python buffer flush
                        logger.info(f"DEBUG: Wrote {len(errors)} errors to file")
                        
                        total_intents += len(intents)
                        total_errors += len(errors)
                        logger.info(f"Found {len(intents)} intents and {len(errors)} errors")
                        
                    except Exception as e:
                        logger.info(f"Error processing token {token_id} in project {project_id}: {str(e)}")

        logger.info(f"\nAnalysis complete. Found {total_intents} total intents and {total_errors} total errors.")

    finally:
        snowflake_client.close()
        analyzer.close()

if __name__ == '__main__':
    main() 