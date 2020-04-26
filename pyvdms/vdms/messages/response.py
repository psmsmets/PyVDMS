r"""

:mod:`vdms.messages.arrival` -- Response Message
===============================================

VDMS response messages class.

"""


# Relative imports
from .message import Message


__all__ = ['Response']


class Response(Message):
    """
    Response message class (VDMS :: RESPONSE).
    """
    def __init__(self, station: str, channel: str, starttime,
                 endtime=None, id: str = None):
        """
        Initiate a VDMS RESPONSE request message.

        Parameters
        ----------

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
        self.set_time(starttime, endtime)
        self.station = station
        self.channel = channel
        self.id = id

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
