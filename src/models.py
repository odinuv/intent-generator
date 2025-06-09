from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum

class ErrorCategory(str, Enum):
    INSUFFICIENT_DATA = "insufficient_data"
    POTENTIAL_SESSION = "potential_session"
    STRANGE_SEQUENCE = "strange_sequence"
    OTHER = "other"

@dataclass
class ConfigurationChange:
    configuration_id: str
    component_id: str
    initial_state: Dict[str, Any]
    final_state: Dict[str, Any]
    is_created: bool
    is_deleted: bool
    event_time: datetime

@dataclass
class ConfigurationRowChange:
    configuration_row_id: str
    configuration_id: str
    component_id: str
    initial_state: Dict[str, Any]
    final_state: Dict[str, Any]
    is_created: bool
    is_deleted: bool
    event_time: datetime

@dataclass
class JobExecution:
    job_id: str
    configuration_id: str
    start_time: datetime
    end_time: datetime
    status: str
    error_message: Optional[str]

@dataclass
class TableEvent:
    event_id: str
    event_type: str
    event_time: datetime
    table_id: str
    message: Optional[str]

@dataclass
class Session:
    start_time: datetime
    end_time: datetime
    token_id: str
    project_id: str
    configuration_changes: List[ConfigurationChange]
    configuration_row_changes: List[ConfigurationRowChange]
    job_executions: List[JobExecution]
    table_events: List[TableEvent]
    is_successful: bool
    session_id: str

@dataclass
class Intent:
    start_time: datetime
    end_time: datetime
    token_id: str
    project_id: str
    configuration_ids: List[str]
    intent_description: str
    is_successful: bool
    session_id: str
    fulfillment: Optional[str] = None
    tags: Optional[List[str]] = None
    classification: Optional[str] = None
    development_stage: Optional[str] = None
    summary: Optional[str] = None

@dataclass
class Error:
    start_time: datetime
    end_time: datetime
    token_id: str
    project_id: str
    configuration_ids: List[str]
    error_category: ErrorCategory
    context: Optional[str] 