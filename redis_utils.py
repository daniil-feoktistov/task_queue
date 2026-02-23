import api
import redis
import json
import jobs

def convert_to_hset(job: jobs.Job):
    payload = {
        "task": job.task,
        "secs_to_complete": job.secs_to_complete,
        "params": json.dumps(job.params)
    }
    return payload