# -*- coding: utf-8 -*-
"""
"""


# Mandatory imports
from obspy import UTCDateTime, Stream, Inventory


# Relative imports
from ..vdms.request import Request
from ..vdms.messages import Chan_status, Sta_info, Waveform


__all__ = ['Client']


class Client(object):
    """
    VDMS client.

    For details see the :meth:`Client.__init__()` method.
    """

    def __init__(self, command_line_client: str = None, **kwargs):
        """
        Initializes a VDMS client.

        >>> client = Client()

        :type command_line_client: str
        :param command_line_client: Base name or full path to the command
        line client.
        """
        self._request = Request(
            message=None, command_line_client=command_line_client, **kwargs
        )

    @property
    def last_request(self):
        """Return the last request object of the client.
        """
        return self._request

    def get_stations(
        self, station: str, channel: str, starttime: UTCDateTime = None,
        endtime: UTCDateTime = None, **kwargs
    ) -> Inventory:
        """
        Query the dataselect service of the client.

        >>> client = Client()
        >>> t1 = UTCDateTime("2010-02-27T06:30:00.000")
        >>> t2 = t1 + 60
        >>> st = client.get_waveforms("I18H1", "BDF", t1, t2)
        >>> print(st)
        1 Trace(s) in Stream:
        IM.I18H1..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples

        The services can deal with UNIX style wildcards.

        >>> st = client.get_waveforms("I18*", "*", t1, t2)
        >>> print(st)
        12 Trace(s) in Stream:
        IM.I18H1..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples
        IM.I18H2..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples
        IM.I18H3..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples
        IM.I18H4..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples
        IM.I18H4..LDA | 2010-02-27T06:30:00.0695Z - ... | 1.0 Hz, 60 samples
        IM.I18H4..LKO | 2010-02-27T06:30:00.0695Z - ... | 1.0 Hz, 60 samples
        IM.I18H4..LWD | 2010-02-27T06:30:00.0695Z - ... | 1.0 Hz, 60 samples
        IM.I18H4..LWS | 2010-02-27T06:30:00.0695Z - ... | 1.0 Hz, 60 samples
        IM.I18L1..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples
        IM.I18L2..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples
        IM.I18L3..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples
        IM.I18L4..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples

        :type station: str
        :param station: Select one or more SEED station codes. Multiple codes
            are comma-separated (e.g. ``"ANMO,PFO"``). Wildcards are allowed.
        :type channel: str
        :param channel: Select one or more SEED channel codes. Multiple codes
            are comma-separated (e.g. ``"BHZ,HHZ"``).
        :type starttime: :class:`~obspy.core.utcdatetime.UTCDateTime`
        :param starttime: Limit results to time series samples on or after the
            specified start time
        :type endtime: :class:`~obspy.core.utcdatetime.UTCDateTime`
        :param endtime: Limit results to time series samples on or before the
            specified end time
        :type format: str
        :param format: Specify status type (inventory or dataframe).

        Any additional keyword arguments will be passed to the service as
        additional arguments. If you pass one of the default parameters and the
        service does not support it, a warning will be issued. Passing any
        non-default parameters that the service does not support will raise
        an error.
        """
        self._request.message = Sta_info(station=station, channel=channel)
        inv = self._request.submit(**kwargs)
        return inv.select(starttime=starttime, endtime=endtime) if inv else inv

    def get_waveforms(
        self, station: str, channel: str, starttime, endtime=None, **kwargs
    ) -> Stream:
        """
        Query the dataselect service of the client.

        >>> client = Client()
        >>> t1 = UTCDateTime("2010-02-27T06:30:00.000")
        >>> t2 = t1 + 60
        >>> st = client.get_waveforms("I18H1", "BDF", t1, t2)
        >>> print(st)
        1 Trace(s) in Stream:
        IM.I18H1..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples

        The services can deal with UNIX style wildcards.

        >>> st = client.get_waveforms("I18*", "*", t1, t2)
        >>> print(st)
        12 Trace(s) in Stream:
        IM.I18H1..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples
        IM.I18H2..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples
        IM.I18H3..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples
        IM.I18H4..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples
        IM.I18H4..LDA | 2010-02-27T06:30:00.0695Z - ... | 1.0 Hz, 60 samples
        IM.I18H4..LKO | 2010-02-27T06:30:00.0695Z - ... | 1.0 Hz, 60 samples
        IM.I18H4..LWD | 2010-02-27T06:30:00.0695Z - ... | 1.0 Hz, 60 samples
        IM.I18H4..LWS | 2010-02-27T06:30:00.0695Z - ... | 1.0 Hz, 60 samples
        IM.I18L1..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples
        IM.I18L2..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples
        IM.I18L3..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples
        IM.I18L4..BDF | 2010-02-27T06:30:00.0695Z - ... | 20.0 Hz, 1200 samples

        :type station: str
        :param station: Select one or more SEED station codes. Multiple codes
            are comma-separated (e.g. ``"ANMO,PFO"``). Wildcards are allowed.
        :type channel: str
        :param channel: Select one or more SEED channel codes. Multiple codes
            are comma-separated (e.g. ``"BHZ,HHZ"``).
        :type starttime: :class:`~obspy.core.utcdatetime.UTCDateTime`
        :param starttime: Limit results to time series samples on or after the
            specified start time
        :type endtime: :class:`~obspy.core.utcdatetime.UTCDateTime`
        :param endtime: Limit results to time series samples on or before the
            specified end time

        Any additional keyword arguments will be passed to the service as
        additional arguments. If you pass one of the default parameters and the
        service does not support it, a warning will be issued. Passing any
        non-default parameters that the service does not support will raise
        an error.
        """
        self._request.message = Waveform(station, channel, starttime, endtime)
        return self._request.submit(**kwargs)

    def get_status(
        self, station: str, channel: str, starttime, endtime=None, **kwargs
    ):
        """

        Parameters:
        -----------

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

        Any additional keyword arguments will be passed to the service as
        additional arguments. If you pass one of the default parameters and the
        service does not support it, a warning will be issued. Passing any
        non-default parameters that the service does not support will raise
        an error.
        """
        self._request.message = Chan_status(
            station, channel, starttime, endtime, **kwargs
        )
        return self._request.submit()
