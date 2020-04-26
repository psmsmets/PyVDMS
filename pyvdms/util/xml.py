r"""

:mod:`util.xml` -- XML utilities
================================

Some general XML helper utilities.

"""


# Mandatory imports
from xml.dom.minidom import Document


__all__ = ['removeElementByTagName']


def removeElementByTagName(xmldoc: Document, tag: str, attr: str = None,
                           values: list = None):
    """Remove an xml element by its tag name, attribute and values.
    """

    if not isinstance(xmldoc, Document):
        raise TypeError('Xmldoc should be of type `xml.dom.minidom.Document`')

    nodes = xmldoc.getElementsByTagName(tag)

    if attr:

        if not isinstance(attr, str):
            raise TypeError('Attribute should be of type str!')

        values = values or []

        if not isinstance(values, list):
            raise TypeError('Attribute values should be of type list!')

        for node in nodes:

            if not node.hasAttribute(attr):

                continue

            if values and node.getAttribute(attr) not in values:

                continue

            parent = node.parentNode
            parent.removeChild(node)

    else:

        for node in nodes:

            parent = node.parentNode
            parent.removeChild(node)
