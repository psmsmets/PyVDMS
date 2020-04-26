**************************************************
PyVDMS - Python Verification Data Messaging System
**************************************************

**PyVDMS** is an open-source project containing multiple wrappers and
extensions to the NMS Client to access IMS data and IDC products of the
Comprehensive Nuclear-Test-Ban Treaty Organization (CTBTO).


Features
========

Main pyvdms features listed per submodule:

- **vdms**: core functionality of the VDMS.

  - ``messages``: VDMS messages derived from the base class ``Message``.
    Implemented messages are [``Arrival``, ``Bulletin``, ``Chan_status``,
    ``Channel``, ``Response``, ``Sta_info``, ``Waveform``].
  - ``request``: VDMS request service wrapping the command line client.
  - ``client``: VDMS messages and request as a client service.

- **filesystem**: high-end functionality to add missing/incomplete waveform
  data to your local archive.

- **jobber**: turns the filesystem functions into a ``queue`` with ``jobs`` to
  handle sequences of requests and overcome out-of-quota issues.

- **scheduler**: a cron-triggered command line client of the jobbed requests.

- **util**: Various utilities.

  - ``headings``: 
  - ``logger``: an extended and pre-configured logging object.
  - ``strlist``: function to check the presence of a substring in a list of
    strings and to extract a specific line (or part of a line).
  - ``verify``: generalized tuple range input validator.
  - ``xml``: remove an xml element by a tag name and optionally by an attribute
    and its values.


Installation
============


Create a clone, or copy of the pyvdms repository

.. code-block:: console

    git clone https://github.com/psmsmets/pyvdms.git

Run ``git pull`` to update the local repository to this master repository.


Install pyvdms via ``pip``:

.. code-block:: console

   cd pyvdms
   pip install -e .


Required are Python version 3.5 or higher and the modules `NumPy`_, `SciPy`_,
`ObsPy`_, and `Pandas`_.
Old versions of `ObsPy`_ (<1.2.0) and `NumPy`_ tend to cause problems which
kills the `remove_response`_ function of `ObsPy`_.
Create a working (non-conflicting) Python 3.7 environment in conda as follows:

.. code-block:: console

    conda env create -f pyvdms.yml


Path
----

Define the `CLIENT_SCHEDULER_HOME` variable in your default bash environment if
you would like to make use of the `client_scheduler` tool to crontab requests
using a prioritized joblist.

.. code-block:: console
    export CLIENT_SCHEDULER_HOME='path to scheduler home'


NMS Client
----------

Make sure that the `nms_client` of CTBTO is installed and configured and that
you have access to the CTBTO's Secure Web Portal.
Credentials for the `nms_client` should be provided in
```$ ~/.nms_client/nms_cred```.

.. code-block:: console
    export NMS_CLI_HOME='path to NMS client home'
    export PATH="${NMS_CLI_HOME}/bin:$PATH"


License information
===================

Copyright 2020 Pieter Smets.

Licensed under the GNU GPLv3 License. See the ``LICENSE``- and ``NOTICE``-files
or the documentation for more information.


Documentation
=============

Messages
--------

Request
-------

Get the status of a specific IMS infrasound array.

```python
from nms_tools.nms_client import Request
request = Request('status')
```

Set the date (or period), station and channel for the status request and submit.
```python
from obspy import UTCDateTime
request.submit(starttime=UTCDateTime(2019, 5, 18), station='I37*', channel='*' )
```

Get the status of the request.
```python
request.status
```

Inspect the actual message that has been sent.
```python
print(request.message)
```

Messages and output files are written to disk in your tmp folder. A new folder is created per request and immediately removed after the request is completed (also on fail). The returned data, if present, is loaded in the `request.result` object.

```python
request.result
```

The logs of the `nms_client` request are wrapped in the object as well.
```python
request.log
```

Re-send the request and only change the station (or any other variable).
```
request.submit(station='I18*')
request.result
```

Manually changing the request message header will initialize the message but will clear all output as it is not yet submitted.
```python
request.set_message(starttime=UTCDateTime())
if request.result:
    print('This is not happening.')
```

### Client - NMS Client as a service

An **obspy.clients** like wrapper for the `nms_client` command line tool.
```python
from nms_tools.nms_client import Client
client = Client()
```

Request the station inventory (`sta_info` request), however, the `SC3XML` format has errors (missing data) failing obspy to read it. 
Providing `format=dataframe` requests the related `chan_stat` instead and returns it as a Pandas dataframe.

```python
inv = client.get_stations( station='I37*', channel='*', format='dataframe' )
inv
```

Request waveforms for the given station, channel and starttime (and endtime, if given).
```python
st = client.get_waveforms( station="I37*",channel="*", starttime=UTCDateTime() )
st.plot()
```
If something goes wrong you can always inspect the last request object.
```python
client.last_request.status
```


waveforms2SDS - Automatic waveform retrieval for your local SDS archive
-----------------------------------------------------------------------

Automatically download waveforms per day and add them to the SDS archive. If
waveforms for a specifc station and channel already exist then these are
skipped. If your SDS archive contains gaps then first the status will be
requested. If no status information is returned and the gap length exceeds the
`force_request_threshold` then the entire day will be (re-) downloaded.

```python
from nms_tools.nms_client import Client2SDS
from obspy import UTCDateTime

resp = Client2SDS(
    starttime = UTCDateTime(2019, 10, 1),
    endtime = UTCDateTime(2019, 10, 31),
    station = 'I18*',
    channel = '*',
    sds_root = 'path_to_your_sds_archive',
    debug = False,
    force_request_threshold = 300., # force to re-download the entire day if no status is returned and 300s or more are missing in the SDS archive
    request_limit = '2GB' # if you want to limit the total request size (an approximation!)
)
if resp.success:
    if resp.completed:
        print('Request completed.')
    elif resp.quota_exceeded:
        print('Quota reached. You should continue the same request from {} onwards.'.format(resp.time))
else:
    print('An error occurred during the request')
    print(resp.error)
```

client_jobber -  a waveforms2SDS scheduling service
---------------------------------------------------
Instead of manually starting `Client2SDS` requests or daily continuing long
requests that are stalled due to quota limitations, requests can be defined
as a `job` and added to the `joblist` for automatic scheduling.
Checkout `client_scheduler` for a cron triggered CLI for Client2SDS requests.

```python
from nms_tools.client_jobber import Job, Joblist
```
A `job` contains all arguments of  `Client2SDS` with additional job-parameters
as the id, priority, user and status  information.
```python
job = Job(
    starttime = '2019-10-01',
    endtime = '2019-10-31',
    station = 'I18*',
    channel = '*',
    sds_root = '~/WaveformArchive',
    priority = 1,
    force_request_threshold = 30., # in seconds
    max_request_size = '1GB'
)
```

Examine the job.
```python
print(job)
```

Modify a  job.
```python
job.update(priority=5)
```

Start the job.
```python
job.process()
```

Create a new joblist and add the previously created job.
```python
joblist = Joblist()
joblist.add(job)
```

Remove a  job from the joblist.
```python
joblist.remove(job)
```

Find a  specific job from the joblist.
```python
job = joblist.find(id='...')
```

Get the first scheduled job with the highest priority on the joblist.
```python
joblist.first
```

Print the joblist.
```python
print(joblist)
```
Note that a `content_hash` is created to prevent manual modification of the joblist and so each job.

The joblist can be stored as a  json file.
```python
joblist.write_lock('jobs.lock')
```

and simply read again.
```python
joblist = Joblist('jobs.lock')
```

Some helper functions get pre-filtered lists from the `Joblist` class.
```python
joblist.list_job_ids() # a list of job ids
joblist.scheduled() # all scheduled jobs
joblist.processing() # all processing jobs
```

pyvdms-scheduler - a cron triggered CLC for waveforms2SDS requests
------------------------------------------------------------------
Check all options using.
```shell
$ client_scheduler help

client_scheduler <action> [-d<dir> -j<job> -s<status> -u<user> -h] [args]
Actions : list, add, cancel, clean, cron:stop, cron:start, cron:restart, cron:info, cron:run, run, reset, info, update, logs, defaults, version, help
Options:
-d,--dir=<homedir>
-j,--job=<job>
-s,--status=<status>
-u,--user=<user>
-h,--help
```
Make sure you set the environment variable `$CLIENT_SCHEDULER_HOME`. This folder contains three files: defaults.json, joblist.lock, and  log.txt.

You can preset default values in the defaults.json file.
```json
{
    "starttime": "yesterday",
    "channel": "??F",
    "sds_root": "path_to_your_sds_archive"
}
```
Possible variables are: *starttime, endtime, station, channel, sds_root, priority, max_request_size, email, client, client_kwargs*.

Test parsing the default variables by running.
```shell
$ client_scheduler defaults
```
These parameters can always overruled when adding a new job.

Add a new job
```shell
$ client_scheduler add station='I45*' priority=5 starttime='yesterday'
```

List all jobs in the queue
```shell
$ client_scheduler list
```
You can filter the list with `--status=<status>` and/or `--user=<user>`.

Examine a  job by it's id `<job>`
```shell
$ client_scheduler info --job=<job>
```
or
```shell
$ client_scheduler info -<job>
```

Change the quota or priority of an existing job.
```shell
$ client_scheduler update --job=<job> max_request_size='3GB' priority=10
```

Cancel a job.
```shell
$ client_scheduler cancel --job=<job>
```

Remove completed  jobs from the queue
```shell
$ client_scheduler clean
```

Start processing the jobqueue.
```shell
$ client_scheduler run
```

Run a specific job from the queue.
```shell
$ client_scheduler run --job=<job>
```

View the logs.
```shell
$ client_scheduler logs
```

Re-schedule jobs that were halted due to errors.
```shell
$ client_scheduler reset
```

Self activate processing the jobqueue using a crontab.
```shell
$ client_scheduler cron:start
```
Stop/remove the crontab.
```shell
$ client_scheduler cron:stop
```
List the crontab command.
```shell
$ client_scheduler cron:info
```