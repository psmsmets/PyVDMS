import pytest
import numpy as np
from numpy.testing import assert_allclose

from pyvdms import util


def test_verify_tuple_range():
    out = util.verify_tuple_range(None, allow_none=True, todict=True)
    assert out == dict(first=None, last=None, step=None, unit=None)

    out = util.verify_tuple_range((0., 10., 1., 'km'), todict=True)
    assert out == dict(first=0., last=10., step=1., unit='km')

    out = util.verify_tuple_range((0., 10., 'km'), todict=True)
    assert out == dict(first=0., last=10., step=None, unit='km')

    out = util.verify_tuple_range((0., 10., 1.), todict=True)
    assert out == dict(first=0., last=10., step=1., unit=None)

    out = util.verify_tuple_range((0., 10., 1.), unit=False, todict=True)
    assert out == dict(first=0., last=10., step=1., unit=None)

    out = util.verify_tuple_range((0., 10., 'km'), step=False, todict=True)
    assert out == dict(first=0., last=10., step=None, unit='km')


def test_strlist_contains():
    strlist = ['foo', 'bar', 'Hello world']
    
    out = util.strlist_contains(None, '')
    assert out == False

    out = util.strlist_contains([], '')
    assert out == False

    out = util.strlist_contains(strlist, 'foo')
    assert out == True

    out = util.strlist_contains(strlist, 'hello')
    assert out == False

    out = util.strlist_contains(strlist, 'hello', False)
    assert out == True


def test_strlist_extract():
    out = util.strlist_extract(None, '')
    assert out == []

    out = util.strlist_extract([], '')
    assert out == []

    strlist = ['foo', 'bar', 'Foo bar']

    out = util.strlist_extract(strlist, 'foo', False)
    assert out == strlist[:1]

    out = util.strlist_extract(strlist, 'bar', False)
    assert out == strlist[1:]

    out = util.strlist_extract(strlist, 'Bar', False)
    assert out == []

    out = util.strlist_extract(strlist, 'Bar', False, case_sensitive=False)
    assert out == strlist[1:]

    out = util.strlist_extract(strlist, 'Foo', split=True)
    assert out == strlist[1:2]

    out = util.strlist_extract(strlist, 'foo', offset=1)
    assert out == strlist[1:2]

    out = util.strlist_extract(strlist, 'foo', offset=2, split=True)
    assert out == strlist[-1:]

    out = util.strlist_extract(strlist, 'foo', offset=-2, case_sensitive=False)
    assert out == strlist[:1]
