

# Mandatory imports
import pandas as pd
from obspy import UTCDateTime
from secrets import token_hex
import sys
import os
import json
import getpass
from humanfriendly import format_size, parse_size
from tabulate import tabulate


# Relative imports
from ..util.time import to_datetime, set_time_range
from ..vdms import Client
from ..filesystem import waveforms2SDS


__all__ = ['Job']

_status_codes = {
    'JOB_PENDING': 'Pending',
    'JOB_CHECK': 'Check',
    'JOB_READY': 'Ready',
    'JOB_SCHEDULED': 'Scheduled',
    'JOB_PROCESSING': 'Processing',
    'JOB_ERROR': 'Error',
    'JOB_CANCELLED': 'Cancelled',
    'JOB_COMPLETED': 'Completed'
}


class Job(object):
    """
    Job object.

    For details see the :meth:`~jobber.jobber.Job.__init__()` method.
    """

    def __init__(
        self, sds_root: str, station: str, channel: str, starttime,
        endtime=None, priority: int = None, request_limit=None,
        user: str = None, id: str = None, time: str = None,
        status: list = None, **kwargs
    ):
        """Job waveform request to your local SDS archive.

        Parameters:
        -----------

        sds_root : `str`
            Path to the local SDS archive.

        station : `str`
            Select one or more SEED station codes. Multiple codes are
            comma-separated (e.g. "ANMO,PFO"). Wildcards are allowed.

        channel : `str`
            Select one or more SEED channel codes. Multiple codes are
            comma-separated (e.g. "BHZ,HHZ,*N"). Wildcards are allowed.

        starttime : various
            Set the start time.

        endtime : various, optional
            Set the end time.

        time : various, optional
            Set the paused job time.

        priority : `int`, optional
            Set the priority. Defaults to 1.

        request_limit : `int`  or `str`, optional
            Limit the total request size, in bytes if `int`. A human readable
            limit can be provided as a string. For example: '2GB'.

        user : `str`, optional
            Set the user of the job.

        id : `str`, optional
            Set the id of the job.

        status : `list`, optional
            Set the job status list.

        Any additional keyword arguments will be passed to the request service.
        """

        # set sds root
        if not isinstance(sds_root, str):
            raise TypeError('sds_root should be of type `str`.')
        self._sds_root = sds_root

        # set station
        if not isinstance(station, str):
            raise TypeError('station should be of type `str`.')
        self._station = station

        # set channel
        if not isinstance(channel, str):
            raise TypeError('channel should be of type `str`.')
        self._channel = channel

        # set times
        self._t0, self._t1 = set_time_range(starttime, endtime)
        self._t = to_datetime(time) if time else None

        # via setters
        self.priority = priority
        self.request_limit = request_limit

        # set id
        id = id or token(5) 
        if not isinstance(id, str):
            raise TypeError('Id should be of type `str`.')
        if not len(id) == 10:
            raise ValueError('Id should be of length 10.')
        self._id = id

        # set user
        user = user or getpass.getuser()
        if not isinstance(user, str):
            raise TypeError('user should be of type `str`.')
        self._user = user

        # set client and extra arguments not in job object
        self._client = 'waveforms2SDS'
        if 'client_kwargs' in kwargs:
            self._client_kwargs = kwargs['client_kwargs']
        else:
            self._client_kwargs = kwargs

        # set status
        self._status = status or []
        if not self._status:
            self._set_status('JOB_PENDING')
        if not self._has_status(['JOB_READY']):
            self._check()

    def __str__(self):
        """Get the JSON formatted job.
        """
        return self.details()

    def _repr_pretty_(self, p, cycle):
        p.text(self.__str__())

    def details(self, history: bool = False):
        """Get the job details.
        """
        table = list(
            (key, val) for key, val in self.to_dict(pop=['status']).items()
        )
        if history:
            table += [('-'*27, '-'*20)]
            table += self.history

        return tabulate(table)

    @property
    def id(self):
        """Get the job id.
        """
        return self._id

    @property
    def starttime(self):
        """Get the job start time.
        """
        return self._t0

    @property
    def endtime(self):
        """Get the job end time.
        """
        return self._t1

    @property
    def time(self):
        """Get the paused job time step.
        """
        return self._t

    @property
    def station(self):
        """Get the job station.
        """
        return self._station

    @property
    def channel(self):
        """Get the job channel.
        """
        return self._channel

    @property
    def sds_root(self):
        """Get the path to the local SDS archive for the job.
        """
        return self._sds_root

    @property
    def priority(self):
        """Get or set the job priority.
        """
        return self._priority

    @priority.setter
    def priority(self, priority: int):
        """
        """
        priority = priority or 1

        if not isinstance(priority, int):
            raise TypeError('priority should be of type `int`.')

        self._priority = priority

    @property
    def request_limit_bytes(self):
        """Get the job request limit in bytes.
        """
        return self._request_limit

    @property
    def request_limit(self):
        """Get the human readable Job request limit.
        """
        return (format_size(self._request_limit) if self._request_limit > 0
                else None)

    @request_limit.setter
    def request_limit(self, request_limit):
        """
        """
        request_limit = request_limit or 0

        if isinstance(request_limit, int):
            self._request_limit = request_limit
        elif isinstance(request_limit, str):
            self._request_limit = parse_size(request_limit)
        else:
            raise TypeError('Request limit should be of type int or str.')

    @property
    def client(self):
        """Get the job client.
        """
        return self._client

    @property
    def client_kwargs(self):
        """Get the job client kwargs.
        """
        return self._client_kwargs

    @property
    def user(self):
        """Get the job user.
        """
        return self._user

    def _has_user(self, users: list):
        """Returns True if the job.user is in users.
        """
        return self.user in users if users else True

    @property
    def status(self):
        """Get the last status of the job.
        """
        return self._status[-1]

    @property
    def statuscode(self):
        """Get the last status code of the job.
        """
        return _status_codes[self.status[1]]

    def _has_statuscode(self, statuslist):
        """Check if the job contains a certain status code.
        """
        return self.statuscode.lower() in statuslist if statuslist else True

    @property
    def ready(self):
        """Returns `True` if the job is ready.
        """
        return self._has_status(['JOB_READY'])

    @property
    def completed(self):
        """Returns `True` if the job is completed.
        """
        return self._has_status(['JOB_COMPLETED'])

    @property
    def error(self):
        """Returns `True` if the job has an error.
        """
        return self._has_status(['JOB_ERROR'])

    @property
    def scheduled(self):
        """Returns `True` if the job is scheduled.
        """
        return self._status[-1][1] == 'JOB_SCHEDULED'

    def schedule(self):
        """Set the job to scheduled.
        """
        return self._set_status('JOB_SCHEDULED')

    @property
    def processing(self):
        """Returns `True` if the job is being processed.
        """
        return self._status[-1][1] == 'JOB_PROCESSING'

    def _has_status(self, statuslist: list):
        """Check if the job passed the status list stages.
        """
        if not statuslist:
            return False
        for status in self._status:
            if status[1] in statuslist:
                return True
        return False

    @property
    def history(self):
        """Returns the full job status history.
        """
        return self._status

    @property
    def paused(self):
        """Returns `True` if the job is paused.
        """
        if not self._t:
            return False
        return self._t > self._t0 and self._t < self._t1

    def pause(self, time):
        """Pause the job at time.
        """
        time = to_datetime(time)

        if time < self._t0 or time > self._t1:
            self._set_status('JOB_ERROR')
            return
        self._t = time
        self.schedule()

    def _check(self):
        """Verify the job validity.
        """
        self._set_status('JOB_CHECK')
        if not os.path.isdir(self._sds_root):
            print("SDS root path\"{}\" does not exist.".format(self._sds_root))
            self._set_status('JOB_ERROR')
            return
        try:
            client = Client()
            inventory = client.get_channels(
                station=self._station,
                channel=self._channel,
                starttime=self._t0,
            )
        except Exception as e:
            self._set_status('JOB_ERROR')
            print(str(e))
            print('Could not retrieve the inventory from '
                  'vdms_client.get_channels(). '
                  'Does your VDMS client actually work?')
            return
        if not isinstance(inventory, pd.DataFrame):
            print("Returned station inventory is invalid.")
            self._set_status('JOB_ERROR')
            return
        self._set_status('JOB_READY')

    def process(self, update_status: bool = True, **kwargs):
        """Process the job.
        """
        try:
            self._t = self._t or self._t0
            if update_status:
                self._set_status('JOB_PROCESSING')
            resp = waveforms2SDS(
                station=self.station,
                channel=self.channel,
                starttime=self._t,
                endtime=self._t1,
                sds_root=self.sds_root,
                request_limit=self.request_limit,
                **self._client_kwargs,
                **kwargs,
            )
            if not resp.success:
                self._set_status('JOB_ERROR')
                self._t = resp.time
            elif resp.completed:
                self._set_status('JOB_COMPLETED')
                self._t = None
            else:
                self.pause(resp.time)
            print(
                "Request {}. Downloaded {}{}. Quota limit {}exceeded."
                .format(
                    resp.status,
                    resp.size,
                    ' of ' + self.request_limit if self.request_limit else '',
                    'not yet ' if resp.quota_remaining else '',
                )
            )
            return resp.quota_remaining
        except Exception as e:
            print(str(e))
            self._set_status('JOB_ERROR')
            print("An error occured while processing job {}:"
                  .format(self.id), sys.exc_info()[0])
            return

    @property
    def is_active(self):
        """Returns `True` if the job is active.
        """
        return not self._has_status(['JOB_ERROR', 'JOB_CANCELLED',
                                     'JOB_COMPLETED'])

    def _set_status(self, status: str):
        """Set the job status.
        """
        if status in _status_codes:
            self._status.append((str(UTCDateTime()), status))
        else:
            raise ValueError('Illegal status "{}"'.format(status))

    def update(self, priority: int = None, request_limit=None, **kwargs):
        """Update job parameters.
        """
        if priority:
            self.priority = priority

        if request_limit:
            self.request_limit = request_limit

    def to_dict(self, keys: list = None, pop: list = None) -> dict:
        """Convert Job to a dictionary.
        """
        d = dict(
            id=self.id,
            starttime=self.starttime.strftime('%Y-%m-%d'),
            endtime=self.endtime.strftime('%Y-%m-%d'),
            time=self.time.strftime('%Y-%m-%d') if self.time else None,
            station=self.station,
            channel=self.channel,
            sds_root=self.sds_root,
            priority=self.priority,
            request_limit=self.request_limit,
            user=self.user,
            client=self.client,
            client_kwargs=self._client_kwargs,
            status=self._status,
        )
        if keys:
            if 'id' in keys:
                keys.remove('id')
            keys.insert(0, 'id')
            d = {key: d[key] for key in keys}
            if 'status' in keys:
                d['status'] = _status_codes[d['status'][-1][1]]
        elif pop:
            for item in pop:
                d.pop(item, None)
        return d

    def to_json(self):
        """Convert Job to json.
        """
        return json.dumps(self.to_dict(), indent=4)


def token(n: int = 5, min_alpha: int = 2):
    """
    Get a token with a minimum number of alpha characters.
    """

    # avoid numeric tokens with only e -> require 2 or more characters.
    while True:

        t = token_hex(n)

        if sum(c.isalpha() for c in t) >= min_alpha:

            break

    return t
