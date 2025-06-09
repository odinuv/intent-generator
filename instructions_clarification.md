# Clarifications to Instructions

## 1. Session Time Windows
- A gap of up to 4 hours is considered a session break
- A gap of 24 hours indicates a new session
- Gaps between 4 and 24 hours are considered "potential sessions" to be examined later
- Initial analysis will focus on data from year 2023
- All dates are stored as strings in format: 2023-12-04T16:48:54+0100
- Sessions can start with any event type, including failed jobs or error events
- Initial test case will process data from December 2023 for token_id "155991_kbc-eu-central-1" and project_id "3082_kbc-eu-central-1"

## 2. Configuration and Configuration Row Changes
- Use `configuration_json` and `configuration_row_json` to determine actual changes
- `change_description_short` can provide additional hints about changes
- Multiple modifications should be squashed into a single difference between initial and final state
- Focus on the `parameters` node under the `configuration` node for actual data changes
- The entire `parameters` node is significant, regardless of its complexity
- Other nodes are considered metadata and not representative of data changes
- Configuration rows have N:1 relationship with configurations:
  - Each configuration row belongs to exactly one configuration
  - A configuration can have 0 to N configuration rows
  - When configuration rows are present, the configuration represents shared data for all its rows
- No dependencies exist between different configurations
- Configuration row changes are automatically recorded in configuration versions
- Configuration row contents are stored in `kbc_component_configuration_row_version` table
- Order of changes is not important - only initial and final states matter
- Partial updates within a session are not tracked
- Version tracking:
  - Order versions by `configuration_created_at` and `configuration_row_created_at` respectively
  - Do not rely on the `last_version` flag as it is unreliable

## 3. Job Status Interpretation
- Failed jobs are considered part of intentional testing or accidental errors
- If a session ends with failed jobs, the user intent was not fulfilled
- Failed jobs are not part of the end user's intended workflow
- Session success criteria:
  - A session is considered successful if at least one configuration change is finished by a successful job
  - Multiple configurations and jobs may exist in a session
  - Only the final job status for each configuration matters
- Session end time is determined by the last event in the session, regardless of its type

## 4. Table Events
Valid event values and their significance:
- Significant events:
  - storage.tableDataPreview
  - storage.tableColumnDeleted
  - storage.tablePrimaryKeyAdded
  - storage.tableImportError
  - storage.tableMetadataSet
  - storage.tablePrimaryKeyDeleted
  - storage.tableSnapshotCreated
  - storage.tableCreated
  - storage.tableDeleted
  - storage.tableColumnAdded
  - storage.tableImportDone
  - storage.tableExported
- Non-significant events:
  - storage.workspaceTableCloned
  - storage.workspaceLoaded
- All significant events are treated with equal importance
- `storage.tableImportError` is treated the same way as a failed job

## 5. LLM Integration
- Use LLM model provided in the `MODEL` env, initially `gemini-1.0-pro` will be provided as the LLM model
- The model name must be in the correct format (e.g., `gemini-1.0-pro`)
- The API key must be valid and have access to the specified model

## 6. Output Format
### output/intents.json
Array of sessions with fields:
- start_date_time (first date in the events considered in the session)
- end_date_time (last date in the events considered in the session)
- token_id
- project_id
- configuration_ids (list of configuration ids affected in the session)
- intent_description (the resulting text describing the user intent for the session)
- is_successful (boolean indicating if the intent was fulfilled)

### output/errors.json
Array of problematic sessions with fields:
- start_date_time (first date in the events considered in the session)
- end_date_time (last date in the events considered in the session)
- token_id
- project_id
- configuration_ids (list of configuration ids affected in the session)
- error_category (error/skip reason)
- context (free-form additional data that may be available)

Error categories:
- insufficient_data
- potential_session
- strange_sequence
- other

Context guidelines:
- Include error message from failed jobs if available
- Include configuration IDs that failed
- Keep context concise and relevant

### Session Directory Structure
Each session is stored in a subdirectory named with its UUID under the output directory. The directory contains:
- `raw_events.csv`: All events in the session, ordered by time
  - event_type: Type of event (config/config_row/job/table)
  - event_time: ISO format timestamp
  - event_data: Full event data in JSON format
- `changes.json`: Chronological list of changes in the session
  - date: ISO format timestamp
  - type: Type of entity (configuration/configuration_row/job/table_event)
  - entity_id: ID of the affected entity
  - change_description: Human-readable description of the change
- `state_changes.json`: Comprehensive view of system state changes
  - created_configurations: List of created configurations with final state
  - modified_configurations: List of modified configurations with initial and final states
  - deleted_configurations: List of deleted configurations with initial state
  - created_configuration_rows: List of created configuration rows with final state
  - modified_configuration_rows: List of modified configuration rows with initial and final states
  - deleted_configuration_rows: List of deleted configuration rows with initial state
  - affected_tables: List of tables with all operations performed on them
  - executed_jobs: List of job executions with status and timing information

## 7. Error Handling
- Create output/errors.json for sessions that:
  - Have insufficient data
  - Contain strange sets of operations
  - Fall into the "potential sessions" category (4-24 hour gaps)
  - Encounter API or integration errors

## 8. Performance
- Performance is not a priority
- Focus on clear and straightforward solution

## 9. Intent Description Guidelines
- Intent descriptions should represent actions
- Focus on data processing operations
- Include whether the intent was successfully fulfilled
- No maximum length for intent text
- Example intents:
  - "The user intended to extract data from MySQL database, and process them via Python transformation"
  - "The user wanted to process the data from existing table X via a Snowflake transformation and write them to Looker"
  - "The user wanted to correct settings of an errored Oracle DB extraction job"
  - "The user wanted to change schedule of the daily reporting orchestration to run twice per day"

## 10. Technical Implementation Notes
- Snowflake identifiers (table and column names) are case-sensitive and must be double-quoted
- All database queries should use double-quoted identifiers to preserve case sensitivity
- The system handles multiple types of events (configurations, jobs, table events) in a single session
- The analysis produces detailed intent descriptions with success/failure status
- Date handling should be consistent throughout the application
- Error handling should be comprehensive and include API integration errors

## 11. State Tracking and Analysis
- Each session maintains a complete record of system state changes
- State changes are tracked at multiple levels:
  - Raw event level (raw_events.csv)
  - Chronological change level (changes.json)
  - Comprehensive state difference level (state_changes.json)
- State tracking focuses on:
  - Configuration and configuration row lifecycle (create/modify/delete)
  - Table operations and their sequence
  - Job execution status and timing
- State differences are calculated by comparing initial and final states within the session
- Multiple modifications to the same entity are squashed into a single difference
- Table operations are tracked chronologically to maintain sequence context
- Job executions include timing information to correlate with other state changes
