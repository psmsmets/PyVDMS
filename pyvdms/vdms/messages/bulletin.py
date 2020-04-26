r"""

:mod:`vdms.messages.event` -- Event Message
=========================================

VDMS event messages class.

"""


# Relative imports
from .message import Message
from ...util.verify import verify_tuple_range


__all__ = ['Bulletin']

_bulletin_types = ['reb', 'seb', 'sel1', 'sel2', 'sel3', 'sseb']
_magnitude_units = [None, 'mb', 'ms', 'ml']


class Bulletin(Message):
    """
    Bulletin message class (VDMS :: BULLETIN).
    """
    def __init__(self, bulletin: str, starttime, endtime=None,
                 latitude: tuple = None, longitude: tuple = None,
                 depth: tuple = None, magnitude: tuple = None,
                 id: str = None):
        """
        Initiate a VDMS BULLETIN request message.

        Parameters
        ----------

        bulletin : {'reb', 'seb', 'sel1', 'sel2', 'sel3', 'sseb'}
            Set the bulletin type.

        starttime : `str` or :class:`~obspy.UTCDateTime`
            Set the start time.

        endtime : `str` or :class:`~obspy.UTCDateTime`, optional
            Set the end time. If `None` (default), the start time is set
            to midnight and the end time to the next day.

        latitude : `tuple`
            Set the latitude range (min, max), both of type `float`,
            in decimal degrees towards the North.

        longitude : `tuple`
            Set the longitude range (min, max), both of type `float`,
            in decimal degrees towards the East.

        magnitude : `tuple`
            The magnitude range (min, max), both of type `float`.
            Optionally set the magnitude unit: (min, max, unit),
            which should be any of {'mb', 'ms', 'ml'}.

        id : `str`, optional
            Specify a message id (max 22 characters). Default a unique hex
            token of 20 characters is generated.

        """
        super().__init__()
        self.set_time(starttime, endtime)
        self.bulletin = bulletin
        self.depth = depth
        self.latitude = latitude
        self.longitude = longitude
        self.magnitude = magnitude
        self.id = id

    @property
    def bulletin(self):
        """Get or set the bulletin type.

        bulletin : {'reb', 'seb', 'sel1', 'sel2', 'sel3', 'sseb'}
            Set the bulletin type.
        """
        return self._bulletin

    @bulletin.setter
    def bulletin(self, bulletin: str):
        """Set bulletin.
        """
        if not isinstance(bulletin, str):
            raise TypeError('bulletin should be of type string!')
        bulletin = bulletin.lower()
        if bulletin not in _bulletin_types:
            raise ValueError('bulletin should be either "{}".'
                             .format('|'.join(_bulletin_types)))
        self._bulletin = bulletin

    @property
    def depth(self):
        """Get or set the depth range.

        depth : `tuple`
            Set the depth range (min, max), both of type `float`,
            in km from the surface.
        """
        return self._depth

    @depth.setter
    def depth(self, value: tuple):
        """Set depth range.
        """
        verify_tuple_range(value, name='Depth', unit=False, step=False)
        self._depth = value

    @property
    def latitude(self):
        """Get or set the latitude range.

        latitude : `tuple`
            Set the latitude range (min, max), both of type `float`,
            in decimal degrees towards the North.
        """
        return self._lat

    @latitude.setter
    def latitude(self, value: tuple):
        """Set latitude range.
        """
        verify_tuple_range(value, name='Latitude', unit=False, step=False)
        self._lat = value

    @property
    def longitude(self):
        """Get or set the longitude range.

        longitude : `tuple`
            Set the longitude range (min, max), both of type `float`,
            in decimal degrees towards the East.
        """
        return self._lon

    @longitude.setter
    def longitude(self, value: tuple):
        """Set longitude range.
        """
        verify_tuple_range(value, name='Longitude', unit=False, step=False)
        self._lon = value

    @property
    def magnitude(self):
        """Get or set the magnitude range.

        magnitude : `tuple`
            The magnitude range (min, max), both of type `float`.
            Optionally set the magnitude unit: (min, max, unit),
            which should be any of {'mb', 'ms', 'ml'}.
        """
        return self._mag

    @magnitude.setter
    def magnitude(self, value: tuple):
        """Set magnitude.
        """
        magnitude = verify_tuple_range(value, name='Magnitude',
                                       step=False, todict=True)
        self._mag = (magnitude['first'], magnitude['last'])
        self._mag_unit = magnitude['unit']

    @property
    def magnitude_unit(self):
        """Get or set the magnitude unit.

        magnitude : {'mb', 'ms', 'ml'}
            Set the magnitude unit.
        """
        return self._mag_unit

    @magnitude_unit.setter
    def magnitude_unit(self, unit: str):
        """Set magnitude unit.
        """
        if unit is None:
            self._mag_unit = None
        else:
            if not isinstance(unit, str):
                raise TypeError('Magnitude unit should be of type string.')
            unit = unit.lower()
            if unit not in _magnitude_units:
                raise TypeError('Magnitude unit should be either "{}".'
                                .format('|'.join(_magnitude_units)))
            self._mag_unit = unit

    @property
    def params(self):
        """Get the formatted VDMS request message parameters.
        """
        out = []

        out += [f'BULL_TYPE {self.bulletin}']
        out += [f'TIME {self.time}']

        if self.depth:
            out += [f'DEPTH {self.depth}']
        if self.latitude:
            out += [f'LAT {self.latitude}']
        if self.longitude:
            out += [f'LON {self.longitude}']
        if self.magnitude:
            out += [f'MAG {self.magnitude}']
        if self.magnitude_unit:
            out += [f'MAG_TYPE {self.magnitude_unit}']

        return "\n".join(out).strip().upper()
