"""
Scheduler
=========

Schedule VDMS services.

"""

###############################################################################
# pyvdms-scheduler
# ----------------

# A cron triggered CLC for waveforms2SDS requests

# Check all options using.
pyvmds-scheduler help

# Make sure you set the environment variable ``$CLIENT_SCHEDULER_HOME``. This
# folder contains three files: defaults.json, queue.lock, and  log.txt.

# You can preset default values in the ``defaults.json`` file.
{
    "starttime": "yesterday",
    "channel": "??F",
    "sds_root": "path_to_your_sds_archive"
}

# Possible variables are: *starttime, endtime, station, channel, sds_root,
# priority, max_request_size, email, client, client_kwargs*.

# Test parsing the default variables by running.
pyvmds-scheduler defaults

# These parameters can always overruled when adding a new job.

# Add a new job
pyvmds-scheduler add station='I45*' priority=5 starttime='yesterday'

# List all jobs in the queue
pyvmds-scheduler list

# You can filter the list with `--status=<status>` and/or `--user=<user>`.

# Examine a  job by it's id `<job>`
pyvmds-scheduler info --job=<job>

# or
pyvmds-scheduler info -<job>

# Change the quota or priority of an existing job.
pyvmds-scheduler update --job=<job> max_request_size='3GB' priority=10

# Cancel a job.
pyvmds-scheduler cancel --job=<job>

# Remove completed  jobs from the queue
pyvmds-scheduler clean

# Start processing the jobqueue.
pyvmds-scheduler run

# Run a specific job from the queue.
pyvmds-scheduler run --job=<job>

# View the logs.
pyvmds-scheduler logs

# Re-schedule jobs that were halted due to errors.
pyvmds-scheduler reset

###############################################################################
# cron-triggered
# --------------

# Self activate processing the jobqueue using a crontab.
pyvmds-scheduler cron:start

# Stop/remove the crontab.
pyvmds-scheduler cron:stop

# List the crontab command.
pyvmds-scheduler cron:info
