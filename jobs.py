from pydantic import BaseModel
from enum import Enum

class Job(BaseModel):
    task: str
    secs_to_complete: int
    params: dict = {}

class JobStatus(Enum):
    PENDING = 0
    RUNNING = 1
    COMPLETE = 2