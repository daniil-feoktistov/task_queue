from fastapi import FastAPI
from itertools import count

import redis
import redis_utils
import jobs
import time

"""
Basic Task Manager API.
"""
COUNTER_KEY = "counter1"

r = redis.Redis()
app = FastAPI()

@app.post("/post_job")
def post_job(job: jobs.Job):
    job_id = "job:%s" % r.incr(COUNTER_KEY, amount=1)
    mapping = redis_utils.convert_to_hset(job)
    mapping["status"] = jobs.JobStatus.PENDING.name
    r.hset(job_id, mapping=mapping)
    r.lpush(jobs.QUEUE, job_id)
    r.zadd(jobs.ALL_JOBS, mapping={job_id: time.time_ns()})
    return job_id

@app.get("/get_jobs")
def get_jobs():
    all_jobs = r.zrange(jobs.ALL_JOBS, 0, -1)
    return {job: r.hget(job, "status") for job in all_jobs}

@app.get("/get_pending_jobs")
def get_jobs():
    return r.lrange(jobs.QUEUE_NAME, 0, -1)

@app.get("/get_job")
def get_job(jobid: str):
    all_data = r.hgetall(jobid)
    print(all_data)
    return all_data

@app.get("/")
def default_return():
    return "Task manager home"

