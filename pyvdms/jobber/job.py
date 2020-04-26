

# Mandatory imports
import pandas as pd
from obspy import UTCDateTime
from secrets import token_hex
import sys
import os
import json
import getpass
from humanfriendly import format_size, parse_size
from dateparser import parse


# Relative imports
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
        self, starttime: str, station: str, channel: str, sds_root: str,
        endtime: str = None, priority: int = 1,
        max_request_size_bytes: int = None, max_request_size: str = None,
        email: list = [], user: str = None, id: str = None, time: str = None,
        status: list = [], client: str = 'waveforms2SDS', **kwargs
    ):
        """
        """
        self._id = id or token_hex(4)
        self._t0 = UTCDateTime(parse(starttime))
        self._t1 = UTCDateTime(parse(endtime)) if endtime else self._t0
        self._t = UTCDateTime(time) if time else None
        self._station = station
        self._channel = channel
        self._sds_root = sds_root
        self._priority = priority
        if max_request_size:
            self._max_request_size_bytes = parse_size(max_request_size)
        else:
            self._max_request_size_bytes = max_request_size_bytes
        self._email = email
        self._user = user or getpass.getuser()
        self._client = client
        if 'client_kwargs' in kwargs:
            self._client_kwargs = kwargs['client_kwargs']
        else:
            self._client_kwargs = kwargs
        self._status = status or []
        if not self._status:
            self._set_status('JOB_PENDING')
        if not self._has_status(['JOB_READY']):
            self.check()

    def __str__(self):
        return self.to_json()

    @property
    def id(self):
        return self._id

    @property
    def starttime(self):
        return self._t0

    @property
    def endtime(self):
        return self._t1

    @property
    def time(self):
        return self._t

    @property
    def station(self):
        return self._station

    @property
    def channel(self):
        return self._channel

    @property
    def sds_root(self):
        return self._sds_root

    @property
    def priority(self):
        return self._priority

    @property
    def max_request_size_bytes(self):
        return self._max_request_size_bytes

    @property
    def max_request_size(self):
        return (format_size(self._max_request_size_bytes)
                if self._max_request_size_bytes else None)

    @property
    def client(self):
        return self._client

    @property
    def client_kwargs(self):
        return self._client_kwargs

    @property
    def email(self):
        return self._email

    @property
    def user(self):
        return self._user

    def _has_user(self, userlist):
        return self.user in userlist if userlist else True

    @property
    def status(self):
        return self._status[-1]

    @property
    def statuscode(self):
        return _status_codes[self.status[1]]

    def _has_statuscode(self, statuslist):
        return self.statuscode.lower() in statuslist if statuslist else True

    @property
    def ready(self):
        return self._has_status(['JOB_READY'])

    @property
    def completed(self):
        return self._has_status(['JOB_COMPLETED'])

    @property
    def error(self):
        return self._has_status(['JOB_ERROR'])

    @property
    def scheduled(self):
        return self._status[-1][1] == 'JOB_SCHEDULED'

    def schedule(self):
        return self._set_status('JOB_SCHEDULED')

    @property
    def processing(self):
        return self._status[-1][1] == 'JOB_PROCESSING'

    def _has_status(self, statuslist):
        if not statuslist:
            return False
        for status in self._status:
            if status[1] in statuslist:
                return True
        return False

    @property
    def history(self):
        return self._status

    @property
    def pauzed(self):
        if not self._t:
            return False
        return self._t > self._t0 and self._t < self._t1

    def pause(self, time: UTCDateTime):
        if time < self._t0 or time > self._t1:
            self._set_status('JOB_ERROR')
            return
        self._t = time
        self.schedule()

    def check(self):
        self._set_status('JOB_CHECK')
        if not os.path.isdir(self._sds_root):
            print("SDS root path\"{}\" does not exist.".format(self._sds_root))
            self._set_status('JOB_ERROR')
            return
        try:
            vdms_client = Client()
            inventory = vdms_client.get_stations(
                station=self._station,
                channel=self._channel,
                starttime=self._t0,
                format='dataframe'
            )
        except Exception as e:
            self._set_status('JOB_ERROR')
            print(str(e))
            print('Could not retrieve the inventory from '
                  'vdms_client.get_stations(). '
                  'Does your VDMS client actually work?')
            return
        if not isinstance(inventory, pd.DataFrame):
            print("Returned station inventory is invalid.")
            self._set_status('JOB_ERROR')
            return
        self._set_status('JOB_READY')

    def process(self, update_status: bool = True):
        try:
            self._t = self._t or self._t0
            if update_status:
                self._set_status('JOB_PROCESSING')
            resp = waveforms2SDS(
                starttime=self._t,
                endtime=self._t1,
                station=self._station,
                channel=self._channel,
                sds_root=self._sds_root,
                request_limit_bytes=self._max_request_size_bytes,
                **self._client_kwargs
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
                    (' of ' + self.max_request_size
                     if self.max_request_size_bytes else ''),
                    'not yet ' if resp.quota_remaining else ''
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
        return not self._has_status(['JOB_ERROR', 'JOB_CANCELLED',
                                     'JOB_COMPLETED'])

    def _set_status(self, status: str):
        if status in _status_codes:
            self._status.append((str(UTCDateTime()), status))
        else:
            raise ValueError('Illegal status "{}"'.format(status))

    def _set_priority(self, priority: int):
        self._priority = priority

    def update(self, priority: int = None, max_request_size_bytes: int = None,
               max_request_size: str = None, **kwargs):
        self._priority = priority or self._priority
        if max_request_size:
            self._max_request_size_bytes = parse_size(max_request_size)
        elif max_request_size_bytes:
            self._max_request_size_bytes = max_request_size_bytes

    def to_dict(self, keys: list = None) -> dict:
        d = dict(
            id=self.id,
            starttime=self.starttime.strftime('%Y-%m-%d'),
            endtime=self.endtime.strftime('%Y-%m-%d'),
            time=self.time.strftime('%Y-%m-%d') if self.time else None,
            station=self.station,
            channel=self.channel,
            sds_root=self.sds_root,
            priority=int(self.priority),
            max_request_size_bytes=(int(self._max_request_size_bytes)
                                    if self._max_request_size_bytes else None),
            max_request_size=(format_size(self._max_request_size_bytes)
                              if self._max_request_size_bytes else None),
            email=self.email,
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
        return d

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)
