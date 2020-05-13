# -*- coding: utf-8 -*-
"""
"""

# Mandatory imports
from obspy import Stream, Inventory, UTCDateTime
from pandas import DataFrame

# Relative imports
from ..util.time import to_datetime, set_time_range
from ..vdms.request import Request
from ..vdms.messages import Channel, Chan_status, Sta_info, Waveform


__all__ = ['Client']


class Client(object):
    """
    VDMS client.

    For details see the :meth:`Client.__init__()` method.
    """

    def __init__(self, command_line_client: str = None, **kwargs):
        """
        Initializes a VDMS client.

        Parameters
        ----------
        command_line_client : `str`
            Set the VDMS request command line client, either the command name
            or the full path. Defaults to 'nms_client'.

        Any additional keyword arguments will be passed to the request client.

        Example
        -------
        >>> from pyvdms import Client
        >>> client = Client()

        """
        self._request = Request(
            message=None, command_line_client=command_line_client, **kwargs
        )

    def __str__(self):
        """Get the formatted VDMS client overview.
        """
        out = []
        out += ['VDMS Webservice Client '
                f'(command line client: {self._request.clc})']
        out += ["Available Services: "
                "'Channel', 'Chan_status', 'Sta_info', 'Waveform'"]
        out += ['']
        out += ['Note: only principal users can request IMS data and IDC '
                'products for the verification of the '
                'Comprehensive Nuclear-Test-Ban Treaty (CTBT).']

        return '\n'.join(out)

    def _repr_pretty_(self, p, cycle):
        p.text(self.__str__())

    @property
    def last_request(self):
        """Return the last request object of the client.
        """
        return self._request

    def get_channels(
        self, station: str, channel: str, starttime=None, endtime=None,
        **kwargs
    ) -> DataFrame:
        """
        Query the stations dataselect service of the client.

        Parameters
        ----------
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

        Any additional keyword arguments will be passed to the request service.

        Example
        -------
        >>> from pyvdms import Client
        >>> client = Client()
        >>> ch = client.get_channels("I18*", "BDF", "2020-02-02")
        >>> print(ch)
          network station channel  ...  instrument    on_date  off_date
        0      IM   I18H1     BDF  ...      MB2000 2016-12-06       NaT
        1      IM   I18H2     BDF  ...      MB2000 2016-12-06       NaT
        2      IM   I18H3     BDF  ...      MB2000 2016-12-06       NaT
        3      IM   I18H4     BDF  ...      MB2000 2016-12-06       NaT
        4      IM   I18L1     BDF  ...      MB2000 2016-12-06       NaT
        5      IM   I18L2     BDF  ...      MB2000 2016-12-06       NaT
        6      IM   I18L3     BDF  ...      MB2000 2016-12-06       NaT
        7      IM   I18L4     BDF  ...      MB2000 2016-12-06       NaT

        """
        self._request.message = Channel(
            station=station, channel=channel
        )

        ch = self._request.submit(**kwargs)

        if starttime or endtime:

            if not endtime:

                start = to_datetime(starttime)
                end = start

            elif not starttime:

                end = to_datetime(endtime)
                start = end

            else:

                start, end = set_time_range(starttime, endtime)

            ch = ch[
                (ch.on_date <= start) &
                (
                    (ch.off_date >= end) | (ch.off_date.isnull())
                )
            ]
        pattern = channel.replace('*', '.*').replace('?', '.')
        ch = ch[ch.channel.str.match(pattern)]

        return ch.reset_index(drop=True)

    def get_stations(
        self, station: str, channel: str, starttime=None, endtime=None,
        **kwargs
    ) -> Inventory:
        """
        Query the stations dataselect service of the client.

        Parameters
        ----------
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

        Any additional keyword arguments will be passed to the request service.

        Example
        -------
        >>> from pyvdms import Client
        >>> client = Client()
        >>> inv = client.get_stations("I18*", "BDF", "2020-02-02")
        >>> print(inv)
        Sending institution: sc3ml import (ObsPy Inventory)
        Contains:
            Networks (1):
                IM
            Stations (9):
                IM.I18DK (Qaanaaq, Greenland)
                IM.I18H1 (Qaanaaq, Greenland)
                IM.I18H2 (Qaanaaq, Greenland)
                IM.I18H3 (Qaanaaq, Greenland)
                IM.I18H4 (Qaanaaq, Greenland)
                IM.I18L1 (Qaanaaq, Greenland)
                IM.I18L2 (Qaanaaq, Greenland)
                IM.I18L3 (Qaanaaq, Greenland)
                IM.I18L4 (Qaanaaq, Greenland)
            Channels (8):
                IM.I18H1..BDF, IM.I18H2..BDF, IM.I18H3..BDF, IM.I18H4..BDF,

        """

        self._request.message = Sta_info(
            station=station, channel=channel
        )

        inv = self._request.submit(**kwargs)

        if inv:

            if starttime and endtime:

                start, end = set_time_range(starttime, endtime)
                inv = inv.select(starttime=UTCDateTime(start),
                                 endtime=UTCDateTime(end))

            elif starttime:

                start = to_datetime(starttime)
                inv = inv.select(starttime=UTCDateTime(start))

            elif endtime:

                end = to_datetime(end)
                inv = inv.select(endtime=UTCDateTime(end))

        return inv

    def get_waveforms(
        self, station: str, channel: str, starttime, endtime=None, **kwargs
    ) -> Stream:
        """
        Query the waveforms dataselect service of the client.

        Parameters
        ----------
        station : `str`
            Select one or more SEED station codes. Multiple codes are
            comma-separated (e.g. "ANMO,PFO"). Wildcards are allowed.

        channel : `str`
            Select one or more SEED channel codes. Multiple codes are
            comma-separated (e.g. "BHZ,HHZ,*N"). Wildcards are allowed.

        starttime : various
            Set the start time.

        endtime : various, optional
            Set the end time. If `None` (default), the start time is set
            to midnight and the end time to the next day.
            If ``endtime`` is of type `int` or `float` it defines the duration
            of the time period, in seconds.

        Any additional keyword arguments will be passed to the request service.

        Example
        -------
        >>> from pyvdms import Client
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

        """

        self._request.message = Waveform(
            station, channel, starttime, endtime
        )

        return self._request.submit(**kwargs)

    def get_status(
        self, station: str, channel: str, starttime, endtime=None, **kwargs
    ) -> DataFrame:
        """
        Query the status dataselect service of the client.

        Parameters
        ----------
        station : `str`
            Select one or more SEED station codes. Multiple codes are
            comma-separated (e.g. "ANMO,PFO"). Wildcards are allowed.

        channel : `str`
            Select one or more SEED channel codes. Multiple codes are
            comma-separated (e.g. "BHZ,HHZ,*N"). Wildcards are allowed.

        starttime : various
            Set the start time.

        endtime : various, optional
            Set the end time. If `None` (default), the start time is set
            to midnight and the end time to the next day.
            If ``endtime`` is of type `int` or `float` it defines the duration
            of the time period, in seconds.

        Any additional keyword arguments will be passed to the request service.

        Example
        -------
        >>> from pyvdms import Client
        >>> status = client.get_status('I18*', '*', '2020-02-02')
        >>> print(status)
           network station channel  %_Recvd  ...  samples      RMS        date
        0    I18DK   I18H1     BDF  100.000  ...  1728000   3064.6  2020-02-02
        1    I18DK   I18H2     BDF  100.000  ...  1728000   3133.7  2020-02-02
        2    I18DK   I18H3     BDF   99.954  ...  1727200   3064.2  2020-02-02
        3    I18DK   I18H4     BDF  100.000  ...  1728000   3020.2  2020-02-02
        4    I18DK   I18H4     LDA  100.000  ...    86400    503.7  2020-02-02
        5    I18DK   I18H4     LKO  100.000  ...    86400    682.9  2020-02-02
        6    I18DK   I18H4     LWD  100.000  ...    86400  67181.2  2020-02-02
        7    I18DK   I18H4     LWS  100.000  ...    86400    955.4  2020-02-02
        8    I18DK   I18L1     BDF  100.000  ...  1728000   3028.4  2020-02-02
        9    I18DK   I18L2     BDF  100.000  ...  1728000   3125.1  2020-02-02
        10   I18DK   I18L3     BDF  100.000  ...  1728000   3256.8  2020-02-02
        11   I18DK   I18L4     BDF  100.000  ...  1728000   3180.7  2020-02-02

        """

        self._request.message = Chan_status(
            station, channel, starttime, endtime
        )

        return self._request.submit(**kwargs)
