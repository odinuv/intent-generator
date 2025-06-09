# Preparations
Initialize an empty python project with src and test structure. Do not write any test yet. The project will connect to a Snowflake database, the credentials are stored
in `.env` file. The following environment values are provided:
- SNOWFLAKE_USER
- SNOWFLAKE_PASSWORD
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_WAREHOUSE
- SNOWFLAKE_DATABASE
- SNOWFLAKE_SCHEMA
- MODEL
- GEMINI_API_KEY

Whenever the instructions are unclear or incomplete, ask clarifying questions.

# Input data
I have data in the following tables:

Table: kbc_component_configuration_version
Columns:
- kbc_component_configuration_id
- kbc_branch_id
- kbc_project_id
- configuration_updated_at
- change_description
- configuration_version
- last_version
- kbc_token_id
- token_id
- token_name
- configuration_json
- change_description_short

Table: kbc_component_configuration_row_version
Columns:
- kbc_component_configuration_row_id
- kbc_component_configuration_id
- kbc_branch_id
- kbc_project_id
- configuration_row_updated_at
- change_description
- configuration_row_version
- last_version
- kbc_token_id
- token_id
- token_name
- configuration_row_json
- change_description_short

Table: kbc_job
- kbc_job_id
- kbc_project_id
- kbc_component_configuration_id
- kbc_branch_id
- branch_type
- kbc_component_id
- transformation_type
- credit_type
- job_run_id
- job_start_at
- job_created_at
- job_status
- error_type
- error_message
- job_run_type
- job_params
- job_mode
- job_type
- kbc_token_id
- token_id
- token_name
- flow
- job_time_credits_used
- job_billed_credits_used
- job_total_time_sec
- job_run_time_sec
- job_network_mb
- job_storage_mb
- ds_backend_size
- dwh_small_ratio
- dwh_medium_ratio
- dwh_large_ratio
- backend_size
- queue_version
- kbc_component_configuration_row_id
- error_message_short
- error_group

Table: kbc_table_event
Columns:
- kbc_table_event_id
- kbc_project_id
- table_id
- kbc_project_table_id
- event_created_at
- event
- event_type
- message
- params
- results
- kbc_token_id
- token_name
- kbc_branch_id
- kbc_data_app_id

All columns are of string type. Dates are formatted like this: 2023-12-03T13:01:24Z.

# Goal
These data represent user interaction with the System. The goal is to analyze the intents of the user interaction with the system.
The user is identified by Token Id. The user session is tied to a single project id. A project can contain any number of configurations, configuration rows, jobs and tables.
The user can do various actions - such as create/delete/modify and configurations rows, run jobs, and create/delete/modify tables. 
Each set of user operations can be represented by a session. (e.g. the user came to the system to re-configure, the MySQL extractor, run its' job and imported new data into some table).
Each session should have some intent - discovering the intentent is the ultimate goal.

# Step 1
First step is to identify the user session. To keep things simple for testing, lets concentrate on finding user session for user identified by token Id = "155991_kbc-eu-central-1" and 
project Id = "3082_kbc-eu-central-1". I propose that we union all dates of the actions from the above tables, to get a sequence of dates. In that sequence we should find continous sequences of actions.
A continuous sequence of actions contains only minimal pauses (the user is thinking or distracted) - tens of minutes at most, while the sequences are separated by large windows of inactivity - days.

# Step 2
Second step is that we should identify changes made in each session, for the purpose of generating the user intent. A typical sequence of operations the user can do in a session might look like this:
- create configuration A 
- create configuration row A
- modify configuration A
- modify configuration row A
- run job C
- create configuration B
- modify configuration B
- modify configuration B
- run job D
- import table data
- modify configuration A
- run job E

From a sequence like this, the important part is the difference between the intial state of the system and the final (within the session) state of the system. So we need to extract
- the list of created configurations and their final state
- the list of modified configuration and their initial and final state
- the list of created configuration rows and their final state
- the list of modified configuration rows and their initial and final state
- the list of affected tables and the operation done
- the list of executed jobs and their status
- the list of deleted configurations and their initial state
- the list of deleted configuration rows and their initial state

# Step 3
Third step is to consolidate the changes made in each session. With the above result, we are interested in desribing the overall change that occured in the system. Use the provided LLM to 
solve this step. E.g. 
- The user crated a MysQL extractor configuration 
- The configuration extracts rows with "Table1" and "Table2"
- The user executed a job that ran the configuration successfully.

# Step 4
Fourth step is to translate the changes made to a user intent. Use the provided LLM to 
solve this step. E.g. with the above example, the intent would be 
"The user intended to extract "Table1" and "Table2" from the MySQL server. The intent was fulfilled successfully".
