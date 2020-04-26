# -*- coding: utf-8 -*-
"""
PyVDMS

**PyVDMS** is an open-source project containing multiple wrappers and
extensions to the NMS Client of the IDC of the CTBTO.

:author:
    Pieter Smets (p.s.m.smets@tudelft.nl)

:copyright:
    Pieter Smets (p.s.m.smets@tudelft.nl)

:license:
    This code is distributed under the terms of the
    GNU General Public License, Version 3
    (https://www.gnu.org/licenses/gpl-3.0.en.html)
"""

# Import some modules
from pyvdms import util

# Import some functions and classes
from pyvdms.vdms import messages, Request, Client
from pyvdms.filesystem import waveforms2SDS

# Make only a selection available to __all__ to not clutter the namespace
# Maybe also to discourage the use of `from PyVDMS import *`.
__all__ = ['util', 'messages', 'Request', 'Client', 'waveforms2SDS']

# Version
try:
    # - Released versions just tags:       1.10.0
    # - GitHub commits add .dev#+hash:     1.10.1.dev3+g973038c
    # - Uncom. changes add timestamp: 1.10.1.dev3+g973038c.d20191022
    from pyvdms.version import version as __version__
except ImportError:
    # If it was not installed, then we don't know the version.
    # We could throw a warning here, but this case *should* be
    # rare. empymod should be installed properly!
    from datetime import datetime
    __version__ = 'unknown-'+datetime.today().strftime('%Y%m%d')
