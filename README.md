# User Intent Analysis

This project analyzes user interactions with the system to identify user intents based on their actions. It processes data from Snowflake database tables and uses LLM to determine the user's intentions.

## Setup

1. Create a `.env` file with the following variables:
```
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
MODEL=gemini/gemini-2.5-pro-preview-03-25
GEMINI_API_KEY=your_api_key
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Project Structure

- `src/` - Source code
  - `session_analyzer.py` - Main session analysis logic
  - `snowflake_client.py` - Snowflake database connection and queries
  - `llm_client.py` - LLM integration for intent analysis
  - `models.py` - Data models and types
- `main.py` - Entry point script
- `output/` - Output files
  - `intents.json` - Analyzed user intents
  - `errors.json` - Error cases and potential sessions

## Usage

Run the analysis:
```bash
python main.py
```

The script will:
1. Connect to Snowflake database
2. Analyze user sessions
3. Generate intent analysis
4. Output results to the `output` directory 