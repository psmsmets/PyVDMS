r"""

:mod:`vdms.messages.station` -- Station Message
===============================================

VDMS station messages class.

"""


# Mandatory imports
from xml.dom.minidom import parse
from tempfile import NamedTemporaryFile
from obspy import read_inventory


# Relative imports
from .message import Message
from ...util.xml import removeElementByTagName


__all__ = ['Sta_info']


class Sta_info(Message):
    """
    Stations message class (VDMS :: STA_INFO).
    """
    def __init__(self, station: str, channel: str, id: str = None):
        """
        Initiate a VDMS STA_INFO request message.

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
        self._format = 'sc3xml'
        self.station = station
        self.channel = channel
        self.id = id

    @property
    def params(self):
        """Get the formatted VDMS request message parameters.
        """
        out = [f'STA_LIST {self.station}']

        if self.channel:
            out += [f'CHAN_LIST {self.channel}']

        return "\n".join(out).strip().upper()

    def handler(self, results: list, **kwargs):
        """Result handler of the VDMS STA_INFO request message.

        Parameters
        ----------

        results : `list`
            List with paths to temporary files or objects. Only the first
            item is used.

        **kwargs
            Additional parameters for :meth:`obspy.read_inventory`.

        Returns
        -------

        result : :class:`obspy.Inventory`
            Inventory with requested station info.

        """
        if not results:
            return None

        xmldoc = parse(results[0])

        badTags = [
            dict(tag='responseFAP'),
            dict(tag='stream', attr='code',
                 values=['azb', 'cb', 'fsb', 'vlb']),
        ]

        for badTag in badTags:
            removeElementByTagName(xmldoc, **badTag)

        with NamedTemporaryFile() as f:
            f.write(xmldoc.toprettyxml().encode())
            inv = read_inventory(f.name, format='SC3ML', **kwargs)

        return inv.select(channel=self.channel)
