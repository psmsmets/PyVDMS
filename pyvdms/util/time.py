r"""

:mod:`util.time` -- Time utilities
==================================

Common time methods.

"""


# Mandatory imports
from pandas import to_datetime as pandas_to_datetime
from pandas.tseries.offsets import DateOffset
from dateparser import parse


__all__ = ['to_datetime', 'set_time_range']


def to_datetime(time):
    r"""Extends :meth:`pandas.to_datetime` with some more date time format
    conversions.

    Parameters
    ----------
    time : mixed
        A string or various datetime object.

    Returns
    -------
    time : :class:`pandas.Timestamp`
        Pandas datetime object.

    """

    if time is None:
        return

    if isinstance(time, object) and hasattr(time, 'datetime'):
        time = time.datetime

    elif isinstance(time, str):
        time = parse(time)

    return pandas_to_datetime(time)


def set_time_range(start, end, implicit: bool = True, next_day: bool = True):
    """Set a time range based on various types.

    Parameters:
    -----------

    start : various
        Set the start time. The type should be any of: `str`,
        :class:`datetime.datetime`, :class:`pandas.Timestamp`,
        :class:`numpy.datetime64` or :class:`obspy.UTCDateTime`.

    end : various
        Set the end time. Same formats as for ``start`` can be used to
        set the explicit time.
        If ``implicit`` is `True` the type can also be `int` or `float`
        such that ``end`` defines the duration.

    implicit : `bool`, optional
        Allow to set the time implicitely when `True` (default):
        - Use current time when start time is `None`.
        - Set start time to midnight and the end time to the next day when
          end time is `None` and ``next_day`` is `True`. If ``next_day`` is
          `False` end time becomes the last second of the day.
        - Use ``end`` as duration when type is either `int` or `float`.
          Negative duration will make ``start`` the end time.

    next_day : `bool`, optional
        If `True` (default), set the end time to the begin of the next day if
        ``implicit`` is `True` and end time is `None`. If `False`, end time
        becomes the last second of the day.

    Returns
    -------

    period : tuple
        A tuple with (start, end), both of type :class:`obspy.UTCDateTime`.

    """
    if not implicit and (start is None or end is None):
        raise ValueError('Start and end time should be defined!')

    start = to_datetime(start) or to_datetime('now')

    if implicit:

        if not end:

            start = start + DateOffset(days=0, normalize=True)
            end = start + DateOffset(days=1)

            if not next_day:

                end = end - DateOffset(seconds=1)

        elif isinstance(end, float) or isinstance(end, int):

            duration = end

            if duration < 0:

                end = start
                start = start + DateOffset(seconds=end, normalize=False)

            else:

                end = start + DateOffset(seconds=end, normalize=False)

        else:

            end = to_datetime(end)

    else:

        end = to_datetime(end)

    if end < start:
        raise ValueError('End time should be after start time!')

    return (start, end)
