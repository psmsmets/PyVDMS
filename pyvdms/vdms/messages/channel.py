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

        columns = {
            'network': (0, 9),
            'station': (10, 15),
            'channel': (16, 19),
            'auxillary': (20, 25),
            'latitude': (26, 34),
            'longitude': (35, 45),
            'coordinate_system': (46, 58),
            'elevation': (59, 64),
            'depth': (65, 70),
            'hang': (71, 78),
            'Vang': (79, 83),
            'sampling_rate': (84, 95),
            'instrument': (96, 102),
            'on_date': (103, 113),
            'off_date': (114, 125),
        }

        df = pd.read_fwf(
            results[0],
            header=0,
            colspecs=list(columns.values()),
            names=list(columns.keys()),
            skiprows=6,
            skipfooter=1,
            date_parser=pd.to_datetime,
            parse_dates=[13, 14],
            **kwargs
        )

        # fill empty network
        df.loc[df['network'].isnull(), 'network'] = 'IM'

        return df
