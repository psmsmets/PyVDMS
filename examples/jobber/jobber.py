"""
Jobber
======

VDMS Jobs and Queues.

"""
from pyvdms.jobber import Job, Queue

###############################################################################
# Jobs
# ----

# Instead of manually starting `waveforms2SDS` requests or daily continuing
# long requests that are stalled due to quota limitations, requests can be
# defined as a `job` and added to the `queue` for automatic scheduling.
# Checkout `pyvmds-scheduler` for a cron triggered CLI for waveforms2SDS
# requests.

# A `job` contains all arguments of  `waveforms2SDS` with additional
# job-parameters as the id, priority, user and status  information.

job = Job(
    starttime='2019-10-01',
    endtime='2019-10-31',
    station='I18*',
    channel='*',
    sds_root='~/WaveformArchive',
    priority=1,
    force_request_threshold=30.,  # in seconds
    max_request_size='1GB'
)

# Examine the job.
print(job)

# Modify a job.
job.update(priority=5)

# Start the job.
job.process()

###############################################################################
# Queues
# ------

# Create a new queue and add the previously created job.
queue = Queue()
queue.add(job)

# Remove a job from the queue.
queue.remove(job)

# Find a specific job from the queue.
job = queue.find(id='...')

# Get the first scheduled job with the highest priority on the queue.
queue.first

# Print the queue.
print(queue)

# Note that a `content_hash` is created to prevent manual modification of the
# queue and so each job.

# The queue can be stored as a json file...
queue.write_lock('jobs.lock')

# ...and simply read again.
queue = Queue('jobs.lock')

# Some helper functions get pre-filtered lists from the `Queue` class.
queue.list_job_ids()  # a list of job ids
queue.scheduled()     # all scheduled jobs
queue.processing()    # all processing jobs
