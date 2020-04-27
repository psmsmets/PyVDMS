**************************************************
PyVDMS - Python Verification Data Messaging System
**************************************************

**PyVDMS** A Python version of the VDMS - web-service which allows principal
users to request IMS data and IDC products for the verification of the
Comprehensive Nuclear-Test-Ban Treaty (CTBT).


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
you would like to make use of the `pyvmds-scheduler` tool to crontab requests
using a prioritized queue.

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

View all implemented messages.

.. code-block:: python3

    from pyvdms import messages
    messages.index()

Create and inspect a message.

.. code-block:: python3

    msg = msg.Chan_status(starttime='2020-02-02', station='I18*', channel='*')
    msg

Each message class has its dedicated parameters and its own docstring.


Request
-------

Init a request.

.. code-block:: python3

    from pyvdms import Request
    request = Request(msg)

Get the request message

.. code-block:: python3

    request.message

Create an empty request

.. code-block:: python3

    request = Request(None)

Set (or update) and get the request

.. code-block:: python3

    request.message = message.Chan_status(starttime='2020-02-02', station='I18*')

Submit the request.

.. code-block:: python3

    request.submit()

Messages and output files are written to disk in your tmp folder. A new folder
is created per request and immediately removed after the request is completed
(also on fail).

Get the status of the request.

.. code-block:: python3

    request.status

Or a full overview.

.. code-block:: python3

    request

The logs of the `nms_client` request are wrapped in the object as well.

.. code-block:: python3

    request.log

Re-send the request and only change the station (or any other variable).

.. code-block:: python3

    request.message.station='I37*'
    request.submit()


Client - NMS Client as a service
--------------------------------

An **obspy.clients** like service of the `nms_client` command line client.

.. code-block:: python3

    from pyvdms import Client
    client = Client()

Get the station inventory:

.. code-block:: python3
    client.get_stations(station='I37*', channel='*')

Request waveforms for the given station, channel and starttime (and endtime, if given).

.. code-block:: python3

    st = client.get_waveforms(station='I37*', channel='BDF', starttime=UTCDateTime())
    st.plot()

If something goes wrong you can always inspect the last request object.

.. code-block:: python3

    client.last_request


waveforms2SDS - Automatic waveform retrieval for your local SDS archive
-----------------------------------------------------------------------

Automatically download waveforms per day and add them to the SDS archive. If
waveforms for a specifc station and channel already exist then these are
skipped. If your SDS archive contains gaps then first the status will be
requested. If no status information is returned and the gap length exceeds the
`force_request_threshold` then the entire day will be (re-) downloaded.

.. code-block:: python3

    from pyvdms import waveforms2SDS
    from obspy import UTCDateTime

    resp = waveforms2SDS(
        starttime = UTCDateTime(2019, 10, 1),
        endtime = UTCDateTime(2019, 10, 31),
        station = 'I18*',
        channel = '*',
        sds_root = 'path_to_your_sds_archive',
        force_request = 300.,
        request_limit = '2GB',
    )
    if resp.success:
        if resp.completed:
            print('Request completed.')
        elif resp.quota_exceeded:
            print('Quota reached. '
                  'You should continue the same request from {} onwards.'
                  .format(resp.time))
    else:
        print('An error occurred during the request')
        print(resp.error)


client_jobber -  a waveforms2SDS scheduling service
---------------------------------------------------
Instead of manually starting `waveforms2SDS` requests or daily continuing long
requests that are stalled due to quota limitations, requests can be defined
as a `job` and added to the `queue` for automatic scheduling.
Checkout `pyvmds-scheduler` for a cron triggered CLI for waveforms2SDS requests.


.. code-block:: python3

    pyvdms.jobber import Job, Joblist

A `job` contains all arguments of  `waveforms2SDS` with additional job-parameters
as the id, priority, user and status  information.


.. code-block:: python3

    job = Job(
        starttime = '2019-10-01',
        endtime = '2019-10-31',
        station = 'I18*',
        channel = '*',
        sds_root = '~/WaveformArchive',
        priority = 1,
        force_request_threshold = 30., # in seconds
        max_request_size = '1GB'

Examine the job.

.. code-block:: python3

    job

Modify a job.


.. code-block:: python3

    job.update(priority=5)

Start the job.


.. code-block:: python3

    job.process()

Create a new queue and add the previously created job.


.. code-block:: python3

    queue = Queue()
    queue.add(job)


Remove a  job from the queue.

.. code-block:: python3

    queue.remove(job)


Find a  specific job from the queue.

.. code-block:: python3

    job = queue.find(id='...')


Get the first scheduled job with the highest priority on the queue.

.. code-block:: python3

    queue.first


Print the queue.

.. code-block:: python3

    print(queue)


Note that a `content_hash` is created to prevent manual modification of the
queue and so each job.

The queue can be stored as a json file.

.. code-block:: python3

    queue.write_lock('jobs.lock')

and simply read again.

.. code-block:: python3


    queue = Joblist('jobs.lock')


Some helper functions get pre-filtered lists from the `Joblist` class.

.. code-block:: python3

    queue.list_job_ids()  # a list of job ids
    queue.scheduled()     # all scheduled jobs
    queue.processing()    # all processing jobs



pyvdms-scheduler - a cron triggered CLC for waveforms2SDS requests
------------------------------------------------------------------

Check all options using.


.. code-block:: shell

    pyvmds-scheduler help

Make sure you set the environment variable ``$CLIENT_SCHEDULER_HOME``. This
folder contains three files: defaults.json, queue.lock, and  log.txt.

You can preset default values in the ``defaults.json`` file.

.. code-block:: json

    {
        "starttime": "yesterday",
        "channel": "??F",
        "sds_root": "path_to_your_sds_archive"
    }


Possible variables are: *starttime, endtime, station, channel, sds_root,
priority, max_request_size, email, client, client_kwargs*.

Test parsing the default variables by running.

.. code-block:: shell

    pyvmds-scheduler defaults


These parameters can always overruled when adding a new job.

Add a new job

.. code-block:: shell

    pyvmds-scheduler add station='I45*' priority=5 starttime='yesterday'


List all jobs in the queue

.. code-block:: shell

    pyvmds-scheduler list


You can filter the list with `--status=<status>` and/or `--user=<user>`.

Examine a  job by it's id `<job>`

.. code-block:: shell

    pyvmds-scheduler info --job=<job>


or

.. code-block:: shell

    pyvmds-scheduler info -<job>

Change the quota or priority of an existing job.

.. code-block:: shell

    pyvmds-scheduler update --job=<job> max_request_size='3GB' priority=10

Cancel a job.

.. code-block:: shell

    pyvmds-scheduler cancel --job=<job>

Remove completed  jobs from the queue

.. code-block:: shell

    pyvmds-scheduler clean

Start processing the jobqueue.

.. code-block:: shell

    pyvmds-scheduler run


Run a specific job from the queue.

.. code-block:: shell

pyvmds-scheduler run --job=<job>


View the logs.

.. code-block:: shell

    pyvmds-scheduler logs


Re-schedule jobs that were halted due to errors.

.. code-block:: shell

    pyvmds-scheduler reset

Self activate processing the jobqueue using a crontab.

.. code-block:: shell

    pyvmds-scheduler cron:start


Stop/remove the crontab.

.. code-block:: shell

    pyvmds-scheduler cron:stop


List the crontab command.

.. code-block:: shell

    pyvmds-scheduler cron:info
