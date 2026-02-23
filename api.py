from fastapi import FastAPI
from itertools import count

import redis
import redis_utils
import jobs

r = redis.Redis()
app = FastAPI()

active_jobs = {}
counter = count()

@app.post("/post_job")
def post_job(job: jobs.Job):
    job_id = "job:%s" % next(counter)
    active_jobs[job_id] = {
        "task": job.task,
        "params": job.params,
        "secs_to_complete": job.secs_to_complete}
    mapping = redis_utils.convert_to_hset(job)
    r.hset(job_id, mapping=mapping)
    return job_id

@app.get("/get_jobs")
def get_jobs():
    return [id for id in active_jobs]

@app.get("/get_job")
def get_job(jobid: str):
    all_data = r.hgetall(jobid)
    print(all_data)
    return all_data

@app.get("/")
def default_return():
    return "Task manager home"

