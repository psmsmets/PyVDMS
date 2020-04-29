r"""

:mod:`vdms.messages.channel` -- Channel Message
===============================================

VDMS channel messages class.

"""


# Mandatory imports
import pandas as pd


# Relative imports
from .message import Message


__all__ = ['Channel']


class Channel(Message):
    """
    Channels message class (VDMS :: CHANNEL).
    """
    def __init__(self, station: str, channel: str, id: str = None, **kwargs):
        """
        Initiate a VDMS CHANNEL request message.

        Parameters
        ----------

        station : `str`
            Select one or more SEED station codes. Multiple codes are
            comma-separated (e.g. "ANMO,PFO"). Wildcards are allowed.

        channel : `str`
            Select one or more SEED channel codes. Multiple codes are
            comma-separated (e.g. "BHZ,HHZ,*N"). Wildcards are allowed.

        id : `str`, optional
            Specify a message id (max 22 characters). Default a unique hex
            token of 20 characters is generated.

        """
        super().__init__()
        self.station = station
        self.channel = channel
        self.id = id

    @property
    def params(self):
        """Get the formatted VDMS request message parameters.
        """
        out = []

        if self.station:
            out += [f'STA_LIST {self.station}']
        if self.channel:
            out += [f'CHAN_LIST {self.channel}']

        return "\n".join(out).strip().upper()

    def handler(self, results: list, **kwargs) -> pd.DataFrame:
        """Result handler of the VDMS CHANNEL request message.

        Parameters
        ----------

        results : `list`
            List with paths to temporary files or objects. Only the first
            item is used.

        **kwargs
            Additional parameters for :meth:`pandas.read_fwf`.

        Returns
        -------

        result : :class:`pandas.DataFrame`
            Dataframe with the requested channel information.

        """

        df = pd.read_fwf(
            results[0],
            header=0,
            colspecs=[(0, 9), (10, 15), (16, 19), (20, 25), (26, 34), (35, 45),
                      (46, 58), (59, 64), (65, 70), (71, 78), (79, 83),
                      (84, 95), (96, 102), (103, 113), (115, 125)],
            names=['network', 'station', 'channel', 'auxillary', 'latitude',
                   'longitude', 'coordinate_system', 'elevation', 'depth',
                   'hang', 'Vang', 'sampling_rate', 'instrument', 'on_date',
                   'off_date'],
            skiprows=6,
            skipfooter=1,
            parse_dates=[13, 14],
            **kwargs
        )

        # date str to datetime
        df['on_date'] = pd.to_datetime(df['on_date'])
        df['off_date'] = pd.to_datetime(df['off_date'])

        # fill empty network
        df.loc[df['network'].isnull(), 'network'] = 'IM'

        return df
