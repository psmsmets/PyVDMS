r"""

:mod:`vdms.messages.status` -- Status Message
===============================================

VDMS status messages class.

"""


# Mandatory imports
import pandas as pd
import os


# Relative imports
from .message import Message
from ...util.strlist import strlist_contains, strlist_extract


__all__ = ['Chan_status']


class Chan_status(Message):
    """
    Status message class (VDMS :: CHAN_STATUS).
    """
    def __init__(self, station: str, channel: str, starttime,
                 endtime=None, id: str = None):
        """
        Initiate a VDMS CHAN_STATUS request message.

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
        self._strftime = '%Y/%m/%d'
        self._max_period = 90 * 86400

        self.set_time(starttime, endtime)
        self.station = station
        self.channel = channel
        self.id = id

    @property
    def params(self):
        """Get the formatted VDMS request message parameters.
        """
        out = []

        out += [f'STA_LIST {self.station}']
        out += [f'CHAN_LIST {self.channel}']
        out += [f'TIME {self.time}']

        return "\n".join(out).strip().upper()

    def handler(self, results: list, **kwargs):
        """Result handler of the VDMS CHAN_STATUS request message.

        Parameters
        ----------

        results : `list`
            List with paths to temporary files or objects.

        **kwargs
            Additional parameters for :meth:`pandas.read_fwf`.

        Returns
        -------

        result : :class:`pandas.DataFrame`
            Dataframe with the requested channel status.

        """
        if not results:
            return None

        df = pd.DataFrame()

        def read(result, date):
            df = pd.read_fwf(
                results[0],
                header=0,
                colspecs=[(0, 9), (10, 15), (16, 19), (21, 28),
                          (29, 36), (37, 44), (45, 50), (51, 60),
                          (61, 70), (71, 82), (83, 95)],
                names=['network', 'station', 'channel', '%_Recvd',
                       '%_AvaUA', '%_Avail', 'gaps', 'samples',
                       'constant', 'mean', 'RMS'],
                skiprows=8,
                skipfooter=1,
                **kwargs
            )
            df.dropna(axis='rows', how='all', inplace=True)
            df['date'] = date
            return df

        for result in results:

            if not os.path.exists(result):
                print(f'Warning: output file {result} does not exist. Skipped')
                continue

            with open(result, 'r') as f:
                lines = [line.rstrip('\n') for line in f]

            if not (
                strlist_contains(lines, self.id) and
                strlist_contains(lines, f'DATA_TYPE {self.type}')
            ):
                raise RuntimeError()

            date = strlist_extract(lines, 'Report period from')[0].split()[0]

            df = df.append(read(result, date), ignore_index=True, sort=False)

        df = df.sort_values(
            by=['date', 'station', 'channel'], ascending=[True, True, True]
        ).reset_index(drop=True)

        return df
