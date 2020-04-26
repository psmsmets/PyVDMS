import pytest
import numpy as np
from numpy.testing import assert_allclose

from pyvdms import messages


def test_index():
    from pyvdms.vdms.messages.base import __all__ as ignore
    index = [m for m in messages.__all__ if m not in ignore]

    out = messages.index(False)
    assert out == index

    out = messages.index(True)
    assert out == [i.lower() for i in index]
