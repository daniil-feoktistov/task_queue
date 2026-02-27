from pydantic import BaseModel
from enum import Enum

ALL_JOBS = "job:all"
QUEUE = "job:queue"
PROCESSING = "job:processing"
LEASES = "job:lease"
LEASE_LENGTH_SECS = 15

class Job(BaseModel):
    task: str
    secs_to_complete: int
    params: dict = {}

class JobStatus(Enum):
    PENDING = 0
    RUNNING = 1
    COMPLETE = 2
    FAILED = 3