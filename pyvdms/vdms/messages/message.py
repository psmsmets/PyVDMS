r"""

:mod:`vdms.messages.base` -- Base Message
=========================================

VDMS base messages class.

"""


# Mandatory imports
from obspy import UTCDateTime
from datetime import datetime
from secrets import token_hex
from slugify import slugify
import string
from warnings import warn


__all__ = ['MessageException', 'Message', 'index']


def index(lowercase: bool = False):
    """
    Returns the implemented messages.
    """
    from ..messages import __all__ as msgs
    return [m.lower() if lowercase else m for m in msgs if m not in __all__]


class MessageException(Exception):
    """
    Base exception for Message classes.
    """
    pass


class Message(object):
    """
    Base Message class for common methods.
    """
    def __init__(self):
        """Initiate a VDMS base request message.
        """
        self._version = 'IMS2.0'
        self._format = None
        self._strftime = '%Y/%m/%d %H:%M:%S'
        self._max_period = None
        self._buffer = None
        self._id = None
        self._token = token_hex(10)  # 10-bit -> 20 characters
        self._t0 = None
        self._t1 = None
        self._stations = []
        self._channels = []

    def __str__(self):
        """Get the formatted VDMS request message.
        """
        return self.message

    def _repr_pretty_(self, p, cycle):
        p.text(self.__str__())

    def _update_triggered(self):
        """
        Function triggered when core parameters are updated.
        """
        pass

    @property
    def id(self):
        """Get the VDMS request message id (formatted) or set a new id.

        id : `str`, optional
            Specify a custom message id.``id`` is slugified using
            :func:`slugify`, handling Unicode and non-safe characters.
            Maximum length is 22 characters.
            Set ``id`` to `None` (default), to use a random 10-bit token.
        """
        return self._id or self._token

    @id.setter
    def id(self, value: str):
        """Set id.
        """
        if value is None:
            self._id = None
        else:
            if not isinstance(value, str):
                raise TypeError('Id should be of type string!')
            self._id = slugify(value, max_length=22, separator='_')

    @property
    def token(self):
        """Get the VDMS request message token.
        """
        return self._token

    @property
    def version(self):
        """Get the VDMS request message version
        """
        return f'{self._version}'.strip().upper()

    @property
    def type(self):
        """Get the full VDMS request message name and format
        """
        fmt = f':{self.format}'.strip() if self.format else ''
        return f'{self.name} {self.version}{fmt}'.strip().upper()

    @property
    def name(self):
        """Get the VDMS request message name
        """
        return self.__class__.__name__

    @property
    def format(self):
        """Get the VDMS request message format
        """
        return self._format

    @property
    def params(self):
        """Get the formatted VDMS request message parameters.
        """
        pass

    @property
    def message(self):
        """Get the formatted VDMS request message.
        """
        msg = ("BEGIN IMS2.0\nMSG_TYPE REQUEST\nMSG_ID {}\n{}\n{}\nSTOP"
               .format(self.id, self.params, self.type))
        return msg

    @property
    def starttime(self):
        """Get the VDMS request message start time.
        """
        return self._t0

    @property
    def endtime(self):
        """Get the VDMS request message end time.
        """
        return self._t1

    @property
    def time(self):
        """Get the formatted VDMS request message time period.
        """
        if self._t0:
            t0 = self.starttime
            t1 = self.endtime
            if self.buffer:
                t0 -= self.buffer
                t1 += self.buffer
            return '{t0} to {t1}'.format(
                t0=t0.strftime(self._strftime),
                t1=t1.strftime(self._strftime),
            )

    def set_time(self, start, end=None):
        """Set the VDMS request message time period.

        Parameters
        ----------

        start : `str` or :class:`~obspy.UTCDateTime`
            Set the start time.

        end : `str`, `float`, or :class:`~obspy.UTCDateTime`, optional
            Set the end time. If `None` (default), the start time is set
            to midnight and the end time to the next day.
            If ``end`` is of type `float` it defines the duration of the time
            period, in seconds (``end = start + end``).

        """
        self._update_triggered()

        if isinstance(start, str):
            start = UTCDateTime(start)

        if not isinstance(start, UTCDateTime):
            raise TypeError('start should be either a string or an'
                            'obspy.UTCDateTime object')

        if end is not None:
            if isinstance(end, str):
                end = UTCDateTime(end)
            elif isinstance(end, int) or isinstance(end, float):
                end = start + end
            if not isinstance(end, UTCDateTime):
                raise TypeError('end should be either a string or an '
                                'obspy.UTCDateTime object')
        if not end or start == end:
            start = UTCDateTime(datetime.combine(
                start.datetime, datetime.min.time()
            ))
            end = start + 86400 - 1/1000.
        else:
            if self._max_period and (end - start) > self.max_period_seconds:
                end = start + self.max_period_seconds
                warn(
                    'Maximum request period of {} days exceeded. '
                    'New request period is from {}'
                    .format(self._max_period, self.time)
                )
        self._t0 = start
        self._t1 = end

    @property
    def station(self):
        """Get or set the VDMS request message station if enabled by
        the message class.

        station : `str`
            Select one or more SEED station codes. Multiple codes are
            comma-separated (e.g. "ANMO,PFO"). Wildcards are allowed.
        """
        if self._stations:
            return ('*' if len(self._stations) == 0
                    else ','.join(map(str, self._stations)))

    @station.setter
    def station(self, value: str = None):
        """Set station.
        """
        if self._stations is None:
            warn(f'station is disabled for {self.__class__.__name__}')
            return
        value = value or '*'
        value = value.translate({ord(c): None for c in string.whitespace})
        self._stations = [] if value == '*' else value.split(',')

    @property
    def channel(self):
        """Get or set the VDMS request message channel if enabled by
        the message class.

        channel : `str`
            Select one or more SEED channel codes. Multiple codes are
            comma-separated (e.g. "BHZ,HHZ,*N"). Wildcards are allowed.
        """
        return ('*' if len(self._channels) == 0
                else ','.join(map(str, self._channels)))

    @channel.setter
    def channel(self, value: str = None):
        """Set channel.
        """
        if self._channels is None:
            warn(f'channel is disabled for {self.__class__.__name__}')
            return
        value = value or '*'
        value = value.translate({ord(c): None for c in string.whitespace})
        self._channels = [] if value == '*' else value.split(',')

    @property
    def buffer(self):
        """Get or set the VDMS request message time buffer if enabled by
        the message class.

        buffer : `float`
            Extend the message time period symmetrically, in seconds.
            Defaults to 0.
        """
        return self._buffer

    @buffer.setter
    def buffer(self, buffer: float):
        """Set buffer.
        """
        if self._buffer is None:
            raise ValueError(
                f'buffer cannot be set for a {self.name} message.'
            )
        if not isinstance(buffer, float):
            raise TypeError('buffer should be float seconds!')
        if buffer < 0:
            raise ValueError('buffer cannot be smaller than 0!')
        self._buffer = buffer

    @property
    def max_period_seconds(self):
        """Get the maximum time period of a VDMS request message, in seconds.
        """
        return self._max_period

    @property
    def max_period_days(self):
        """Get the maximum time period of a VDMS request message, in days.
        """
        return int(self._max_period / 86400) if self._max_period else None

    def handler(self, results: list, **kwargs):
        """Result handler of the VDMS request message.

        Parameters
        ----------

        results : `list`
            List with paths to temporary files.

        **kwargs
            Additional parameters to inject in the underlying handler.

        Returns
        -------

        result : various
            Handled results.

        """
        warn('No specific message handler implemented. '
             'Raw request data is returned.')

        lines = []

        for result in results:
             with open(result) as f:
                 lines += f.readlines()

        return lines
