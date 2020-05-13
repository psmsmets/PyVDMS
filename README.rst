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
    Implemented messages and their return types:

    - Arrival : Pandas.DataFrame
    - Bulletin : Obspy.Event
    - Chan_status : Pandas.DataFrame
    - Channel : Pandas.DataFrame
    - Response : Pandas.DataFrame
    - Sta_info : Obspy.Inventory
    - Waveform : Obspy.Stream

  - ``request``: VDMS request service wrapping the command line client.

  - ``client``: VDMS messages and request as a client service.

- **filesystem**: high-end functionality to add missing/incomplete waveform
  data to your local archive.

- **jobber**: turns the filesystem functions into a ``queue`` with ``jobs`` to
  handle sequences of requests and overcome out-of-quota issues.

- **scheduler**: a cron-triggered command line client of the jobbed requests.

- **util**: Various utilities.

  - ``headings``: construct various string headings following the Python’s
    Style Guide for documenting.

  - ``logger``: an extended and pre-configured logging object.

  - ``strlist``: function to check the presence of a substring in a list of
    strings and to extract a specific line (or part of a line).

  - ``verify``: generalized tuple range input validator.

  - ``time``: extension of ``pandas.to_datetime`` and a common method to set time ranges.

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


Required are Python version 3.6 or higher and the modules NumPy, SciPy,
ObsPy, and Pandas.
Old versions of ObsPy (<1.2.0) cnan conflict with NumPy and tend to cause
problems which kills the remove_response function of ObsPy.

Create a working (non-conflicting) Python 3.7 environment in conda as follows:

.. code-block:: console

    conda env create -f environment.yml


Path
----

``pyvmds-scheduler`` to crontab requests using a prioritized queue
stores by default the queue, default settings and logs in ``~/.pyvdms``.

Define the `PYVDMS_HOME` variable in your default bash environment if you would
like to redefine the ``pyvdms-scheduler`` home directory.

.. code-block:: console

    export PYVDMS_HOME='path\to\pyvdms\scheduler\home'


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
Licensed under the GNU GPLv3 License.
