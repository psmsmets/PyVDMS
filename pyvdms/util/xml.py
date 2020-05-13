r"""

:mod:`util.xml` -- XML utilities
================================

Some general XML helper utilities.

"""


# Mandatory imports
from xml.dom.minidom import Document
import re


__all__ = ['removeElementByTagName', 'removeElementAttribute']


def removeElementByTagName(xmldoc: Document, tag: str, attr: str = None,
                           regex: str = None, invert: bool = False):
    """Remove an xml element by its tag name, attribute and values.
    """

    if not isinstance(xmldoc, Document):
        raise TypeError('Xmldoc should be of type `xml.dom.minidom.Document`.')

    nodes = xmldoc.getElementsByTagName(tag)

    if attr:

        if not isinstance(attr, str):
            raise TypeError('Attribute should be of type str!')

        regex = regex or '.'  # match all

        if not isinstance(regex, str):
            raise TypeError('Regex should be of type str!')

        r = re.compile(regex)

        for node in nodes:

            if not node.hasAttribute(attr):

                continue

            if bool(r.match(node.getAttribute(attr))) is invert:

                continue

            parent = node.parentNode
            parent.removeChild(node)

    else:

        for node in nodes:

            parent = node.parentNode
            parent.removeChild(node)


def removeElementAttribute(xmldoc: Document, tag: str, attr: str, regex: str,
                           invert: bool = False):
    """Remove an xml element by its tag name, attribute and values.
    """

    if not isinstance(xmldoc, Document):
        raise TypeError('Xmldoc should be of type `xml.dom.minidom.Document`')

    if not isinstance(attr, str):
        raise TypeError('Attribute should be of type str!')

    regex = regex or '.'  # match all

    if not isinstance(regex, str):
        raise TypeError('Regex should be of type str!')

    r = re.compile(regex)

    nodes = xmldoc.getElementsByTagName(tag)

    for node in nodes:

        if not node.hasAttribute(attr):

            continue

        if bool(r.match(node.getAttribute(attr))) is invert:

            continue

        node.removeAttribute(attr)
