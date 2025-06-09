import json
import csv
import uuid
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
import os
from .models import (
    Session, ConfigurationChange, ConfigurationRowChange,
    JobExecution, TableEvent, Intent, Error, ErrorCategory
)
from .snowflake_client import SnowflakeClient
from .llm_client import LLMClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SessionAnalyzer:
    def __init__(self):
        self.snowflake_client = SnowflakeClient()
        self.llm_client = LLMClient()
        self.session_break_threshold = timedelta(hours=4)
        self.new_session_threshold = timedelta(hours=24)
        self.output_dir = 'output'

    def analyze_user_sessions(self, token_id: str, project_filter: str, start_date: datetime, end_date: datetime) -> Tuple[List[Intent], List[Error]]:
        # Get all events
        config_versions = self.snowflake_client.get_configuration_versions(token_id, project_filter, start_date, end_date)
        config_row_versions = self.snowflake_client.get_configuration_row_versions(token_id, project_filter, start_date, end_date)
        jobs = self.snowflake_client.get_jobs(token_id, project_filter, start_date, end_date)
        if len(jobs) == 0:
            logger.info(f"No jobs found in project {project_filter} for token {token_id} between {start_date} and {end_date}")
            return [], []
        table_events = self.snowflake_client.get_table_events(token_id, project_filter, start_date, end_date)

        # Filter out storage.tableMetadataSet and storage.workspaceTableCloned events
        table_events = [event for event in table_events if event['event'] not in ['storage.tableMetadataSet', 'storage.workspaceTableCloned']]

        # Combine all events and sort by time
        all_events = []
        for event in config_versions:
            all_events.append(('config', datetime.fromisoformat(event['configuration_updated_at'].replace('Z', '+00:00')), event))
        for event in config_row_versions:
            all_events.append(('config_row', datetime.fromisoformat(event['configuration_row_updated_at'].replace('Z', '+00:00')), event))
        for event in jobs:
            all_events.append(('job', datetime.fromisoformat(event['job_created_at'].replace('Z', '+00:00')), event))
        for event in table_events:
            all_events.append(('table', datetime.fromisoformat(event['event_created_at'].replace('Z', '+00:00')), event))

        all_events.sort(key=lambda x: x[1])

        # Identify sessions
        sessions = self._identify_sessions(all_events, token_id, None)  # project_id will be extracted from events
        logger.info(f"DEBUG: Found {len(sessions)} sessions in project {project_filter} for token {token_id}")
        
        # Analyze each session
        intents = []
        errors = []
        for session in sessions:
            try:
                intent = self._analyze_session(session)
                intents.append(intent)
            except Exception as e:
                error = self._create_error(session, str(e))
                errors.append(error)

        logger.info(f"DEBUG: Found {len(intents)} intents and {len(errors)} errors in session {session.session_id}")
        return intents, errors

    def _identify_sessions(self, events: List[Tuple[str, datetime, Dict]], token_id: str, project_id: str) -> List[Session]:
        if not events:
            return []

        sessions = []
        current_session_events = []
        last_event_time = None

        for event_type, event_time, event_data in events:
            if last_event_time is None:
                current_session_events.append((event_type, event_time, event_data))
            else:
                time_diff = event_time - last_event_time
                if time_diff > self.new_session_threshold:
                    # Create new session
                    if current_session_events:
                        session = self._create_session(current_session_events, token_id, project_id)
                        sessions.append(session)
                    current_session_events = [(event_type, event_time, event_data)]
                elif time_diff > self.session_break_threshold:
                    # Potential session break
                    if current_session_events:
                        session = self._create_session(current_session_events, token_id, project_id)
                        sessions.append(session)
                    current_session_events = [(event_type, event_time, event_data)]
                else:
                    # Continue current session
                    current_session_events.append((event_type, event_time, event_data))
            last_event_time = event_time

        # Add the last session
        if current_session_events:
            session = self._create_session(current_session_events, token_id, project_id)
            sessions.append(session)

        return sessions

    def _create_session(self, events: List[Tuple[str, datetime, Dict]], token_id: str, project_id: str) -> Session:
        # Generate session UUID
        session_id = str(uuid.uuid4())
        session_dir = os.path.join(self.output_dir, session_id)
        os.makedirs(session_dir, exist_ok=True)

        # Save raw events to CSV
        csv_path = os.path.join(session_dir, 'raw_events.csv')
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow(['event_type', 'event_time', 'event_data'])
            # Write events
            for event_type, event_time, event_data in events:
                writer.writerow([event_type, event_time.isoformat(), json.dumps(event_data)])

        config_changes = []
        config_row_changes = []
        job_executions = []
        table_events = []

        for event_type, event_time, event_data in events:
            if event_type == 'config':
                config_changes.append(self._create_config_change(event_data))
            elif event_type == 'config_row':
                config_row_changes.append(self._create_config_row_change(event_data))
            elif event_type == 'job':
                job_executions.append(self._create_job_execution(event_data))
            elif event_type == 'table':
                table_events.append(self._create_table_event(event_data))

        # Determine if session was successful
        is_successful = any(
            job.status == 'success' for job in job_executions
        ) and not any(
            job.status == 'error' for job in job_executions
        )

        session = Session(
            start_time=events[0][1],
            end_time=events[-1][1],
            token_id=token_id,
            project_id=project_id,
            configuration_changes=config_changes,
            configuration_row_changes=config_row_changes,
            job_executions=job_executions,
            table_events=table_events,
            is_successful=is_successful,
            session_id=session_id
        )

        # Save changes to JSON
        changes = []
        for change in config_changes:
            changes.append({
                'date': change.event_time.isoformat(),
                'type': 'configuration',
                'entity_id': change.configuration_id,
                'change_description': f"Configuration {change.configuration_id} was {'created' if change.is_created else 'modified' if not change.is_deleted else 'deleted'}"
            })
        
        for change in config_row_changes:
            changes.append({
                'date': change.event_time.isoformat(),
                'type': 'configuration_row',
                'entity_id': change.configuration_row_id,
                'change_description': f"Configuration row {change.configuration_row_id} was {'created' if change.is_created else 'modified' if not change.is_deleted else 'deleted'}"
            })

        for job in job_executions:
            changes.append({
                'date': job.start_time.isoformat(),
                'type': 'job',
                'entity_id': job.job_id,
                'change_description': f"Job {job.job_id} was executed with status {job.status}"
            })

        for event in table_events:
            changes.append({
                'date': event.event_time.isoformat(),
                'type': 'table_event',
                'entity_id': event.event_id,
                'change_description': f"Table event {event.event_id}: {event.event_type} - {event.message or ''}"
            })

        # Sort changes by date
        changes.sort(key=lambda x: x['date'])

        # Save changes to JSON file
        changes_path = os.path.join(session_dir, 'changes.json')
        with open(changes_path, 'w') as f:
            json.dump(changes, f, indent=2)

        return session

    def _create_config_change(self, event_data: Dict) -> ConfigurationChange:
        config_json = json.loads(event_data['configuration_json'])
        # Extract component_id from configuration_id (format: project_id_component_id_configuration_number)
        config_id_parts = event_data['kbc_component_configuration_id'].split('_')
        component_id = config_id_parts[2] if len(config_id_parts) > 2 else 'unknown'
        return ConfigurationChange(
            configuration_id=event_data['kbc_component_configuration_id'],
            component_id=component_id,
            initial_state=config_json,
            final_state=config_json,
            is_created=False,  # Will be updated in _analyze_session
            is_deleted=False,  # Will be updated in _analyze_session
            event_time=datetime.fromisoformat(event_data['configuration_updated_at'].replace('Z', '+00:00'))
        )

    def _create_config_row_change(self, event_data: Dict) -> ConfigurationRowChange:
        config_row_json = json.loads(event_data['configuration_row_json'])
        # Extract component_id from configuration_id (format: project_id_component_id_configuration_number)
        config_id_parts = event_data['kbc_component_configuration_id'].split('_')
        component_id = config_id_parts[2] if len(config_id_parts) > 2 else 'unknown'
        return ConfigurationRowChange(
            configuration_row_id=event_data['kbc_component_configuration_row_id'],
            configuration_id=event_data['kbc_component_configuration_id'],
            component_id=component_id,
            initial_state=config_row_json,
            final_state=config_row_json,
            is_created=False,  # Will be updated in _analyze_session
            is_deleted=False,  # Will be updated in _analyze_session
            event_time=datetime.fromisoformat(event_data['configuration_row_updated_at'].replace('Z', '+00:00'))
        )

    def _create_job_execution(self, event_data: Dict) -> JobExecution:
        return JobExecution(
            job_id=event_data['kbc_job_id'],
            configuration_id=event_data['kbc_component_configuration_id'],
            start_time=datetime.fromisoformat(event_data['job_start_at'].replace('Z', '+00:00')),
            end_time=datetime.fromisoformat(event_data['job_created_at'].replace('Z', '+00:00')),
            status=event_data['job_status'],
            error_message=event_data.get('error_message')
        )

    def _create_table_event(self, event_data: Dict) -> TableEvent:
        return TableEvent(
            event_id=event_data['kbc_table_event_id'],
            event_type=event_data['event'],
            event_time=datetime.fromisoformat(event_data['event_created_at'].replace('Z', '+00:00')),
            table_id=event_data['table_id'],
            message=event_data.get('message')
        )

    def _analyze_session(self, session: Session) -> Intent:
        # Track state changes
        state_changes = {
            'created_configurations': [],
            'modified_configurations': [],
            'deleted_configurations': [],
            'created_configuration_rows': [],
            'modified_configuration_rows': [],
            'deleted_configuration_rows': [],
            'affected_tables': [],
            'executed_jobs': []
        }

        # Process configuration changes
        config_states = {}  # Track initial and final states for each config
        for change in session.configuration_changes:
            if change.configuration_id not in config_states:
                config_states[change.configuration_id] = {
                    'initial': change.initial_state,
                    'final': change.final_state,
                    'is_created': change.is_created,
                    'is_deleted': change.is_deleted,
                    'component_id': change.component_id
                }
            else:
                config_states[change.configuration_id]['final'] = change.final_state
                config_states[change.configuration_id]['is_deleted'] = change.is_deleted

        # Categorize configuration changes
        for config_id, state in config_states.items():
            if state['is_created']:
                state_changes['created_configurations'].append({
                    'id': config_id,
                    'final_state': state['final'],
                    'component_id': state['component_id']
                })
            elif state['is_deleted']:
                state_changes['deleted_configurations'].append({
                    'id': config_id,
                    'initial_state': state['initial'],
                    'component_id': state['component_id']
                })
            else:
                state_changes['modified_configurations'].append({
                    'id': config_id,
                    'initial_state': state['initial'],
                    'final_state': state['final'],
                    'component_id': state['component_id']
                })

        # Process configuration row changes
        row_states = {}  # Track initial and final states for each row
        for change in session.configuration_row_changes:
            if change.configuration_row_id not in row_states:
                row_states[change.configuration_row_id] = {
                    'initial': change.initial_state,
                    'final': change.final_state,
                    'is_created': change.is_created,
                    'is_deleted': change.is_deleted,
                    'config_id': change.configuration_id,
                    'component_id': change.component_id
                }
            else:
                row_states[change.configuration_row_id]['final'] = change.final_state
                row_states[change.configuration_row_id]['is_deleted'] = change.is_deleted

        # Categorize configuration row changes
        for row_id, state in row_states.items():
            if state['is_created']:
                state_changes['created_configuration_rows'].append({
                    'id': row_id,
                    'config_id': state['config_id'],
                    'final_state': state['final'],
                    'component_id': state['component_id']
                })
            elif state['is_deleted']:
                state_changes['deleted_configuration_rows'].append({
                    'id': row_id,
                    'config_id': state['config_id'],
                    'initial_state': state['initial'],
                    'component_id': state['component_id']
                })
            else:
                state_changes['modified_configuration_rows'].append({
                    'id': row_id,
                    'config_id': state['config_id'],
                    'initial_state': state['initial'],
                    'final_state': state['final'],
                    'component_id': state['component_id']
                })

        # Process table events
        table_operations = {}  # Track operations for each table
        for event in session.table_events:
            table_id = event.table_id
            if table_id not in table_operations:
                table_operations[table_id] = []
            table_operations[table_id].append({
                'event_type': event.event_type,
                'message': event.message,
                'time': event.event_time.isoformat()
            })

        # Add table operations to state changes
        for table_id, operations in table_operations.items():
            state_changes['affected_tables'].append({
                'id': table_id,
                'operations': operations
            })

        # Process job executions
        for job in session.job_executions:
            state_changes['executed_jobs'].append({
                'id': job.job_id,
                'config_id': job.configuration_id,
                'status': job.status,
                'error_message': job.error_message,
                'start_time': job.start_time.isoformat(),
                'end_time': job.end_time.isoformat()
            })

        # Save state changes to session directory
        session_dir = os.path.join(self.output_dir, session.session_id)
        state_changes_path = os.path.join(session_dir, 'state_changes.json')
        with open(state_changes_path, 'w') as f:
            json.dump(state_changes, f, indent=2)

        # Process state changes into detailed descriptions
        processed_changes = {
            'configuration_changes': [],
            'configuration_row_changes': [],
            'table_operations': [],
            'job_executions': []
        }

        # Process configuration changes
        for config in state_changes['created_configurations']:
            config_data = config['final_state']
            description = f"Created configuration {config['id']} of type {config['component_id']}"
            if 'parameters' in config_data:
                description += f" with parameters: {json.dumps(config_data['parameters'])}"
            processed_changes['configuration_changes'].append(description)

        for config in state_changes['modified_configurations']:
            initial = config['initial_state']
            final = config['final_state']
            description = f"Modified configuration {config['id']} of type {config['component_id']}"
            if 'parameters' in initial and 'parameters' in final:
                description += f". Changes in parameters: {json.dumps(final['parameters'])}"
            processed_changes['configuration_changes'].append(description)

        for config in state_changes['deleted_configurations']:
            config_data = config['initial_state']
            description = f"Deleted configuration {config['id']} of type {config['component_id']}"
            processed_changes['configuration_changes'].append(description)

        # Process configuration row changes
        for row in state_changes['created_configuration_rows']:
            row_data = row['final_state']
            description = f"Created configuration row {row['id']} for configuration {row['config_id']} of type {row['component_id']}"
            if 'parameters' in row_data:
                description += f" with parameters: {json.dumps(row_data['parameters'])}"
            processed_changes['configuration_row_changes'].append(description)

        for row in state_changes['modified_configuration_rows']:
            initial = row['initial_state']
            final = row['final_state']
            description = f"Modified configuration row {row['id']} for configuration {row['config_id']} of type {row['component_id']}"
            if 'parameters' in initial and 'parameters' in final:
                description += f". Changes in parameters: {json.dumps(final['parameters'])}"
            processed_changes['configuration_row_changes'].append(description)

        for row in state_changes['deleted_configuration_rows']:
            row_data = row['initial_state']
            description = f"Deleted configuration row {row['id']} for configuration {row['config_id']} of type {row['component_id']}"
            processed_changes['configuration_row_changes'].append(description)

        # Process table operations
        for table in state_changes['affected_tables']:
            for operation in table['operations']:
                description = f"Table {table['id']}: {operation['event_type']}"
                if operation['message']:
                    description += f" - {operation['message']}"
                processed_changes['table_operations'].append(description)

        # Process job executions
        for job in state_changes['executed_jobs']:
            description = f"Job {job['id']} for configuration {job['config_id']} executed with status {job['status']}"
            if job['error_message']:
                description += f". Error: {job['error_message']}"
            processed_changes['job_executions'].append(description)

        # Save processed changes to session directory
        processed_changes_path = os.path.join(session_dir, 'state_changes_processed.json')
        with open(processed_changes_path, 'w') as f:
            json.dump(processed_changes, f, indent=2)

        # Summarize configuration and configuration row changes using LLM
        config_summary = {
            'created_configurations': [],
            'modified_configurations': [],
            'created_configuration_rows': [],
            'modified_configuration_rows': []
        }

        # Group configurations by component type
        configs_by_type = {}
        for config in state_changes['created_configurations']:
            component_type = config['component_id']
            if component_type not in configs_by_type:
                configs_by_type[component_type] = []
            configs_by_type[component_type].append(config)

        # Summarize created configurations by component type
        for component_type, configs in configs_by_type.items():
            if len(configs) == 1:
                config = configs[0]
                description = f"Created a {component_type} configuration"
                if 'parameters' in config['final_state']:
                    description += f" with parameters: {json.dumps(config['final_state']['parameters'])}"
                config_summary['created_configurations'].append(description)
            else:
                description = f"Created {len(configs)} {component_type} configurations"
                config_summary['created_configurations'].append(description)

        # Group modified configurations by component type
        configs_by_type = {}
        for config in state_changes['modified_configurations']:
            component_type = config['component_id']
            if component_type not in configs_by_type:
                configs_by_type[component_type] = []
            configs_by_type[component_type].append(config)

        # Summarize modified configurations by component type
        for component_type, configs in configs_by_type.items():
            if len(configs) == 1:
                config = configs[0]
                description = f"Modified a {component_type} configuration"
                if 'parameters' in config['final_state']:
                    description += f" with updated parameters: {json.dumps(config['final_state']['parameters'])}"
                config_summary['modified_configurations'].append(description)
            else:
                description = f"Modified {len(configs)} {component_type} configurations"
                config_summary['modified_configurations'].append(description)

        # Group configuration rows by parent configuration
        rows_by_config = {}
        for row in state_changes['created_configuration_rows']:
            config_id = row['config_id']
            if config_id not in rows_by_config:
                rows_by_config[config_id] = []
            rows_by_config[config_id].append(row)

        # Summarize created configuration rows by parent configuration
        for config_id, rows in rows_by_config.items():
            if len(rows) == 1:
                row = rows[0]
                description = f"Created a configuration row for configuration {config_id}"
                if 'parameters' in row['final_state']:
                    description += f" with parameters: {json.dumps(row['final_state']['parameters'])}"
                config_summary['created_configuration_rows'].append(description)
            else:
                description = f"Created {len(rows)} configuration rows for configuration {config_id}"
                config_summary['created_configuration_rows'].append(description)

        # Group modified configuration rows by parent configuration
        rows_by_config = {}
        for row in state_changes['modified_configuration_rows']:
            config_id = row['config_id']
            if config_id not in rows_by_config:
                rows_by_config[config_id] = []
            rows_by_config[config_id].append(row)

        # Summarize modified configuration rows by parent configuration
        for config_id, rows in rows_by_config.items():
            if len(rows) == 1:
                row = rows[0]
                description = f"Modified a configuration row for configuration {config_id}"
                if 'parameters' in row['final_state']:
                    description += f" with updated parameters: {json.dumps(row['final_state']['parameters'])}"
                config_summary['modified_configuration_rows'].append(description)
            else:
                description = f"Modified {len(rows)} configuration rows for configuration {config_id}"
                config_summary['modified_configuration_rows'].append(description)

        # Save configuration summary to session directory
        config_summary_path = os.path.join(session_dir, 'config_summary.json')
        with open(config_summary_path, 'w') as f:
            json.dump(config_summary, f, indent=2)

        # Get intent description from LLM using processed changes and configuration summary
        intent_description = self.llm_client.analyze_session(session, processed_changes, config_summary)

        # Classify intent fulfillment and session categories
        fulfillment = self._classify_intent_fulfillment(session, processed_changes, config_summary, intent_description)
        tags, classification, development_stage = self._classify_session_categories(session, processed_changes, config_summary, intent_description)
        
        # Generate one sentence intent summary
        summary = self._generate_intent_summary(session, processed_changes, config_summary, intent_description)

        return Intent(
            start_time=session.start_time,
            end_time=session.end_time,
            token_id=session.token_id,
            project_id=session.project_id,
            configuration_ids=[change.configuration_id for change in session.configuration_changes],
            intent_description=intent_description,
            is_successful=session.is_successful,
            session_id=session.session_id,
            fulfillment=fulfillment,
            tags=tags,
            classification=classification,
            development_stage=development_stage,
            summary=summary
        )

    def _classify_intent_fulfillment(self, session: Session, processed_changes: Dict, config_summary: Dict, intent_description: str) -> str:
        """
        Classify the intent success into one of three categories:
        - Successful Completion - Intent fully achieved
        - Partial Success - Some components worked, others failed
        - Failed with Troubleshooting - Active problem-solving attempts
        """
        fulfillment_prompt = f"""
        Analyze this user session and classify the outcome into exactly one of these categories:

        1. "Successful Completion" - Intent fully achieved, all major components worked as expected
        2. "Partial Success" - Some components worked, others failed, mixed results
        3. "Failed with Troubleshooting" - Active problem-solving attempts, debugging activities

        Session details:
        - Session successful: {session.is_successful}
        - Intent description: {intent_description}
        - Job executions: {processed_changes.get('job_executions', [])}
        - Configuration changes: {processed_changes.get('configuration_changes', [])}
        - Table operations: {processed_changes.get('table_operations', [])}

        Job statuses in session:
        {[job.status for job in session.job_executions]}

        Return only one of the three exact category names: "Successful Completion", "Partial Success", or "Failed with Troubleshooting"
        """

        return self.llm_client.get_completion(fulfillment_prompt).strip().strip('"')

    def _classify_session_categories(self, session: Session, processed_changes: Dict, config_summary: Dict, intent_description: str) -> Tuple[List[str], str, str]:
        """
        Classify the session into Primary Goal and Development Stage categories, and generate meaningful intent tags:
        
        Primary Goal:
        - Ad-hoc analysis/Data exploration/inspection
        - ETL/ELT pipeline setup/Data export/sharing
        - Troubleshooting/Debugging
        
        Development Stage:
        - Creating new use cases
        - Updating existing use cases
        - Testing/validating configurations
        
        Returns: (tags, classification, development_stage)
        """
        categorization_prompt = f"""
        Analyze this user session and provide:

        1. PRIMARY GOAL (choose exactly one):
        - "Ad-hoc analysis/Data exploration/inspection"
        - "ETL/ELT pipeline setup/Data export/sharing"
        - "Troubleshooting/Debugging"

        2. DEVELOPMENT STAGE (choose exactly one):
        - "Creating new use cases"
        - "Updating existing use cases"
        - "Testing/validating configurations"

        3. INTENT TAGS (list 2-4 meaningful tags that describe the intent):
        Generate descriptive tags that capture the essence of what the user was trying to accomplish.
        Use short, descriptive phrases that would be useful for categorizing and searching intents.

        Session details:
        - Intent description: {intent_description}
        - Configuration changes: {config_summary}
        - Job executions: {processed_changes.get('job_executions', [])}
        - Table operations: {processed_changes.get('table_operations', [])}
        - Session successful: {session.is_successful}

        Configuration states:
        - Created configurations: {len(config_summary.get('created_configurations', []))}
        - Modified configurations: {len(config_summary.get('modified_configurations', []))}

        Interacting with the Keboola.sandbox component suggests ad-hoc analysis/Data exploration/inspection, but may
        also be used for Troubleshooting/Debugging.

        Examples of good intent tags:
        - "data-extraction", "datbase-source", "api-source"
        - "pipeline-setup", "automation", "etl"
        - "troubleshooting", "connection-error", "mongodb"
        - "data-transformation", "filtering", "date-range"
        - "configuration-update", "parameter-change"
        - "data-validation", "testing", "quality-check"

        Return your answer in this exact format:
        PRIMARY_GOAL: [exact category name]
        DEVELOPMENT_STAGE: [exact category name]
        INTENT_TAGS: [tag1], [tag2], [tag3], [tag4]
        """

        response = self.llm_client.get_completion(categorization_prompt).strip()
        
        # Parse the response
        lines = response.split('\n')
        primary_goal = None
        development_stage = None
        tags = []
        
        for line in lines:
            if line.startswith('PRIMARY_GOAL:'):
                primary_goal = line.split(':', 1)[1].strip().strip('"')
            elif line.startswith('DEVELOPMENT_STAGE:'):
                development_stage = line.split(':', 1)[1].strip().strip('"')
            elif line.startswith('INTENT_TAGS:'):
                tags_str = line.split(':', 1)[1].strip()
                # Parse comma-separated tags
                tags = [tag.strip().strip('"') for tag in tags_str.split(',') if tag.strip()]
        
        # Use primary goal as the main classification
        classification = primary_goal if primary_goal else "Unknown"
        development_stage = development_stage if development_stage else "Unknown"
        
        return tags, classification, development_stage

    def _generate_intent_summary(self, session: Session, processed_changes: Dict, config_summary: Dict, intent_description: str) -> str:
        """
        Generate a 1-3 sentence summary of what the user wanted to do, written from their perspective.
        """
        summary_prompt = f"""
        Create a concise summary (1-3 sentences) describing what the user wanted to accomplish in this session. 
        Write from the user's perspective using first person ("I want to...", "I need to...", "I am trying to...").

        Session details:
        - Intent description: {intent_description}
        - Session successful: {session.is_successful}
        - Configuration changes: {len(session.configuration_changes)} changes
        - Job executions: {len(session.job_executions)} jobs
        - Table events: {len(session.table_events)} events

        Job results: {[job.status for job in session.job_executions]}

        Focus on describing the user's goals and intentions, not the technical implementation details or outcomes.

        Examples of good summaries:
        - "I want to extract data from my MySQL database and load it into Snowflake for analysis."
        - "I need to set up automated data extraction from MongoDB to create regular reports."
        - "I want to modify my existing data transformations to filter out older records and only include data from 2023 onwards."
        - "I am trying to troubleshoot my data pipeline because it keeps failing during the extraction step."

        Return only the 1-3 sentence summary from the user's perspective, no additional text.
        """

        return self.llm_client.get_completion(summary_prompt).strip().strip('"')

    def _create_error(self, session: Session, error_message: str) -> Error:
        return Error(
            start_time=session.start_time,
            end_time=session.end_time,
            token_id=session.token_id,
            project_id=session.project_id,
            configuration_ids=[change.configuration_id for change in session.configuration_changes],
            error_category=ErrorCategory.OTHER,
            context=error_message
        )

    def close(self):
        self.snowflake_client.close() 