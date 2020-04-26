r"""

:mod:`vdms.messages.arrival` -- Arrival Message
===============================================

VDMS arrival messages class.

"""


# Relative imports
from .message import Message


__all__ = ['Arrival']

_arrival_types = ['automatic', 'grouped', 'associated', 'unassociated']


class Arrival(Message):
    """
    Arrivals message class (VDMS :: ARRIVAL).
    """
    def __init__(self, arrival: str, station: str, channel: str, starttime,
                 endtime=None, id: str = None):
        """
        Initiate a VDMS ARRIVAL request message.

        Parameters
        ----------

        arrival : {'automatic', 'grouped', 'associated', 'unassociated'}
            Set the arrival type.

        station : `str`
            Select one or more SEED station codes. Multiple codes are
            comma-separated (e.g. "ANMO,PFO"). Wildcards are allowed.

        channel : `str`
            Select one or more SEED channel codes. Multiple codes are
            comma-separated (e.g. "BHZ,HHZ,*N"). Wildcards are allowed.

        starttime : `str` or :class:`~obspy.UTCDateTime`
            Set the start time.

        endtime : `str` or :class:`~obspy.UTCDateTime`, optional
            Set the end time. If `None` (default), the start time is set
            to midnight and the end time to the next day.

        id : `str`, optional
            Specify a message id (max 22 characters). Default a unique hex
            token of 20 characters is generated.

        """
        super().__init__()
        self.arrival = arrival
        self.set_time(starttime, endtime)
        self.station = station
        self.channel = channel
        self.id = id

    @property
    def arrival(self):
        """Get or set the arrival type.

        arrival : {'automatic', 'grouped', 'associated', 'unassociated'}
            Set the arrival type. Defaults to 'automatic'.
        """
        return self._bulletin

    @arrival.setter
    def arrival(self, arrival: str):
        """Set arrival.
        """
        if not isinstance(arrival, str):
            raise TypeError('arrival should be of type string!')
        arrival = arrival.lower()
        if arrival not in _arrival_types:
            raise ValueError('arrival should be either "{}".'
                             .format('|'.join(_arrival_types)))
        self._arrival = arrival

    @property
    def params(self):
        """Get the formatted VDMS request message parameters.
        """
        out = []

        if self.time:
            out += [f'TIME {self.time}']
        if self.station:
            out += [f'STA_LIST {self.station}']
        if self.channel:
            out += [f'CHAN_LIST {self.channel}']

        return "\n".join(out).strip().upper()

    @property
    def name(self):
        """Get the VDMS request message name
        """
        return f'{self.__class__.__name__}:{self._arrival}'
