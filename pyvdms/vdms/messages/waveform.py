r"""

:mod:`vdms.messages.waveform` -- Waveform Message
===============================================

VDMS waveform messages class.

"""


# Mandatory imports
from obspy import read, Stream


# Relative imports
from .message import Message


__all__ = ['Waveform']


class Waveform(Message):
    """
    Waveforms message class (VDMS :: WAVEFORM).
    """
    def __init__(self, station: str, channel: str, starttime,
                 endtime=None, id: str = None):
        """
        Initiate a VDMS WAVEFORM request message.

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
        self._strftime = '%Y-%m-%d %H:%M:%S.%f'
        self._format = 'ms_st2_512'
        self._buffer = 0.

        self.set_time(starttime, endtime)
        self.station = station
        self.channel = channel
        self.id = id

    @property
    def params(self):
        """Get the formatted VDMS request message parameters.
        """
        out = []

        out += [f'TIME {self.time}']
        out += [f'STA_LIST {self.station}']
        out += [f'CHAN_LIST {self.channel}']

        return "\n".join(out).strip().upper()

    def handler(self, results: list, **kwargs):
        """Result handler of the VDMS WAVEFORM request message.

        Parameters
        ----------

        results : `list`
            List with paths to temporary files or objects.

        **kwargs
            Additional parameters for :meth:`obspy.read`.

        Returns
        -------

        result : :class:`obspy.Stream`
            Stream with requested waveforms.

        """
        if not results:
            return None

        st = Stream()
        for result in results:
            st += read(result, **kwargs)

        return st
