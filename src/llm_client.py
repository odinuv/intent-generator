import os
import json
from typing import List, Dict, Any
import google.generativeai as genai
from dotenv import load_dotenv
from .models import Session

load_dotenv()

class LLMClient:
    def __init__(self):
        genai.configure(api_key=os.getenv('GEMINI_API_KEY'))
        # Use gemini-1.0-pro as the default model
        model_name = os.getenv('MODEL', 'gemini-1.0-pro')
        self.model = genai.GenerativeModel(model_name)

    def analyze_session(self, session: Session, processed_changes: Dict[str, List[str]], config_summary: Dict[str, List[str]]) -> str:
        # Prepare the session data for the LLM
        session_data = {
            "start_time": session.start_time.isoformat(),
            "end_time": session.end_time.isoformat(),
            "token_id": session.token_id,
            "project_id": session.project_id,
            "is_successful": session.is_successful,
            "configuration_changes": [
                {
                    "configuration_id": change.configuration_id,
                    "is_created": change.is_created,
                    "is_deleted": change.is_deleted,
                    "initial_state": change.initial_state,
                    "final_state": change.final_state
                }
                for change in session.configuration_changes
            ],
            "configuration_row_changes": [
                {
                    "configuration_row_id": change.configuration_row_id,
                    "configuration_id": change.configuration_id,
                    "is_created": change.is_created,
                    "is_deleted": change.is_deleted,
                    "initial_state": change.initial_state,
                    "final_state": change.final_state
                }
                for change in session.configuration_row_changes
            ],
            "job_executions": [
                {
                    "job_id": job.job_id,
                    "configuration_id": job.configuration_id,
                    "start_time": job.start_time.isoformat(),
                    "end_time": job.end_time.isoformat(),
                    "status": job.status,
                    "error_message": job.error_message
                }
                for job in session.job_executions
            ],
            "table_events": [
                {
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "event_time": event.event_time.isoformat(),
                    "table_id": event.table_id,
                    "message": event.message
                }
                for event in session.table_events
            ],
            "processed_changes": processed_changes,
            "config_summary": config_summary
        }

        # Create the prompt for the LLM
        prompt = f"""
        Analyze the following user session data and provide a comprehensive, detailed description of the user's intent and its fulfillment. 
        
        Focus on:
        1. **Multi-step processes**: Describe the full journey including initial attempts, failures, debugging steps, and iterative refinements
        2. **Nuanced success assessment**: Distinguish between partial success, complete success, and specific failure points
        3. **Elements integration**: Mention all data sources, transformations, and destinations involved
        4. **Iterative workflows**: Recognize trial-and-error processes, configuration adjustments, and troubleshooting patterns

        Session Data:
        {json.dumps(session_data, indent=2)}

        Provide a comprehensive intent description that captures:
        - What the user was trying to achieve (be specific about data sources, destinations, transformations)
        - The technical challenges encountered (specific error messages, configuration issues)
        - The iterative process of refinement and debugging
        - Whether different parts of the intent were fulfilled or failed
        - The overall outcome and success assessment

        Examples of the detailed style expected:
        - "The user intended to extract data from a MongoDB database and a MySQL database. The data extraction from MySQL (into table `in.aims_searches`) was successful, but the repeated attempts to extract data from MongoDB failed due to a persistent server version incompatibility (Server at 127.0.0.1:33006 reports wire version 6, but this version of  requires at least 7), meaning this part of the intent was not fulfilled."
        
        - "The user intended to modify two existing Snowflake transformations to filter their output tables to include only data from 2023 onwards. This involved adding new SQL filtering logic and then debugging syntax errors (specifically, correcting a placeholder column name to the actual \"date\" column in the `WHERE` clause). The intent to correctly apply these date filters was successfully fulfilled after iterative corrections, as evidenced by subsequent successful job executions for both transformations and their downstream dependencies."
        
        Write a single comprehensive paragraph (or multiple paragraphs if the session is complex) that thoroughly describes the user's intent and its fulfillment.
        """

        # Get the response from the LLM
        response = self.model.generate_content(prompt)
        return response.text.strip()

    def get_completion(self, prompt: str) -> str:
        """
        Get a completion from the LLM for a given prompt.
        """
        response = self.model.generate_content(prompt)
        return response.text.strip()