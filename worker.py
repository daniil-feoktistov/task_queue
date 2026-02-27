import redis
import logging
import time
import jobs
import random 
import traceback
import signal

from typing import Dict, Optional

MAX_JOB_RUNTIME = 3600
IDLE_LOG_INTERVAL = 5
WORKER_FAILURE_RATE = 0.2
JOB_FAILURE_RATE = 0.2

# --------------------------
# TODO: Things to Test
# - Graceful shutdown
# - Worker succeeds
# - Worker fails
# --------------------------

# TODO: Implement supervisor/worker pool and real worker crash mechanics.
# TODO: Implement real heartbeat using threading and optionally Redis lease.

# --------------------------
# Set up logging
# --------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("worker")

# --------------------------
# Graceful shutdown
# --------------------------

class ShutdownRequested(Exception):
    pass

class InternalWorkerFailure(Exception):
    pass

_shutdown = False

def _handle_signal(signum, frame):
    global _shutdown
    _shutdown = True
    logger.warning("Shutdown requested (signal=%s). Finishing current job if any." % signum)

# --------------------------
# Running a job
# --------------------------

def simulate_worker_failure(failure_rate: int=0) -> bool:
    """
    Simulates worker failure.
    """
    # Simulates worker failure.
    if random.random() < failure_rate:
        # Simulate raising an exception.
        return True
    return False

def run_job(job_data, failure_rate=0.0):
    """
    Simulates running a job.
    """
    if random.random() < failure_rate:
        time.sleep(2)
        logger.warning("Simulated task failure.")
        raise RuntimeError("Simulated task failure.")
    
    secs = parse_secs_to_complete(job_data)
    time.sleep(secs)

# --------------------------
# Redis interactions
# --------------------------

def simulate_heartbeat(r: redis.Redis, job_id: str) -> None:
    """
    Simulates a heartbeat by setting a new LEASE_LENGTH_SECS-length lease for the given job ID.
    """
    r.hset(jobs.LEASES, mapping={job_id: time.time()+jobs.LEASE_LENGTH_SECS})

def reserve_job(r: redis.Redis, queue: str, processing: str, timeout: int=3) -> Optional[str]:
    """
    Reserve a job from the given Redis queue and move it to a processing queue. Returns the reserved job's job_id.
    """
    job = r.brpoplpush(queue, processing, timeout)
    return job

def load_job_data(r: redis.Redis, job_id: str) -> Dict[str, str]:
    """
    Loads job metadata for a given job_id from Redis.
    """
    return r.hgetall(job_id)

def mark_status(r: redis.Redis, job_id: str, status: jobs.JobStatus, extra_payload: Optional[Dict[str, str]] = None) -> None:
    """
    Update a job's status, and add extra information (e.g. failure info) if necessary.
    """
    mapping = {"status": status.name, "last_updated": str(int(time.time()))}
    if extra_payload:
        mapping.update(extra_payload)
    r.hset(job_id, mapping=mapping)

def parse_secs_to_complete(job_data: Dict[str, str]) -> int:
    """
    Parse [secs_to_complete] from job_data dict.
    """
    raw = job_data.get('secs_to_complete')
    # Raise error if dict does not contain value
    if raw is None:
        raise ValueError("Missing required field 'secs to complete'.")
    # Raise error if value cannot be converted to int
    try:
        secs = int(raw)
    except ValueError as e:
        raise e
    # Raise error if value outside of expected range
    if secs < 0 or secs > MAX_JOB_RUNTIME:
        raise ValueError("secs_to_complete (%s) outside of range [0,%s]" % (secs, MAX_JOB_RUNTIME))
    return secs

def remove_job_from_processing_queue(r: redis.Redis, processing: str, job_id: str):
    """
    Remove the given job_id from a processing queue.
    """
    r.lrem(name=processing, count=1, value=job_id)
    logger.info("Removed task %s from PROCESSING queue." % job_id)

# --------------------------
# Main loop
# --------------------------

def run_forever():
    """
    Worker loop -- pulls a job, attempts to run it, and updates Redis according to whether or not the job succeeded.
    """
    r = redis.Redis(decode_responses=True)
    last_idle_log = time.time()

    while True:
        # If _handle_signal() has been triggered since the last iteration, close out the worker.
        if _shutdown:
            raise ShutdownRequested()
        
        # Move a job from QUEUE to PROCESSING
        job_id = reserve_job(r, jobs.QUEUE, jobs.PROCESSING)

        # Log if no item is in the Redis queue, but only once at a time.
        if job_id is None:
            if time.time() > last_idle_log + IDLE_LOG_INTERVAL:
                logger.info("No item in Redis queue.")
                last_idle_log = time.time()
            continue
        else:
            simulate_heartbeat(r, job_id)

        if simulate_worker_failure(failure_rate=WORKER_FAILURE_RATE):
            raise InternalWorkerFailure("Simulated worker crash.")

        job_data = load_job_data(r, job_id)
        logger.info("Working on task %s", job_id)
        mark_status(r, job_id, jobs.JobStatus.RUNNING)
        
        try:
            # Attempt to run a job. if the job works, mark status as COMPLETE.
            run_job(job_data, failure_rate=JOB_FAILURE_RATE)
            mark_status(r, job_id, jobs.JobStatus.COMPLETE)
        except Exception as e:
            # If the job fails, mark the status as FAILED and save the error trace.
            failure_info = {"error": str(e), "trace": traceback.format_exc()}
            mark_status(r, job_id, jobs.JobStatus.FAILED, extra_payload=failure_info)
        finally:
            # Either way, remove the job from the processing queue.
            logger.info("Finished processing job %s." % job_id)
            remove_job_from_processing_queue(r, jobs.PROCESSING, job_id)

# --------------------------
# main()
# --------------------------

def main():
    # Register signal handlers for SIGINT and SIGTERM
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        run_forever()
    except ShutdownRequested:
        logger.info("Worker stopped.")
        return 0
    except InternalWorkerFailure:
        logger.error("Worker crashed.")
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    raise SystemExit(main())