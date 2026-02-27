import redis
import logging
import jobs
import time

# --------------------------
# Set up logging
# --------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reaper")

# --------------------------
# Redis interactions
# --------------------------

def worker_died(r: redis.Redis, job_id: str) -> bool:
    """
    Check to see if a job lapsed its processing lease.
    """
    job_lease = r.hget(jobs.LEASES, job_id)
    # TODO: Need to come up with more airtight logic for what happens if a job was pulled but never got a lease.
    return job_lease is None or time.time() > float(job_lease)

def handle_dead_worker(r: redis.Redis, job_id: str, queue: str, processing: str, leases: str) -> None:
    r.lrem(processing, 1, job_id)
    r.lpush(queue, job_id)
    r.hdel(leases, job_id)

# --------------------------
# Main loop
# --------------------------

def run_forever():
    """
    Reaper job that checks PROCESSING for crashed workers and moves them back to the queue.
    """
    r = redis.Redis(decode_responses=True)
    while True:
        processing = r.lrange(jobs.PROCESSING, 0, -1)
        logging.warning("Processing: %s" % processing)
        for job_id in processing:
            if worker_died(r, job_id):
                logging.warning("WORKER HANDLING JOB ID %s DIED" % job_id)
                handle_dead_worker(r, job_id, jobs.QUEUE, jobs.PROCESSING, jobs.LEASES)
        time.sleep(3)

if __name__ == "__main__":
    run_forever()