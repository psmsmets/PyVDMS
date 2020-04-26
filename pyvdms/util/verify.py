r"""

:mod:`util.verify` -- Input verification
========================================

Common input verification methods.

"""


# Mandatory imports
import numpy as np

__all__ = ['verify_tuple_range']


def verify_tuple_range(
    input_range: tuple, allow_none: bool = True, name: str = None,
    step: bool = None, unit: bool = None, todict: bool = False
):
    """
    Verify if the input range tuple fullfils the requirements.
    An error is raised if a criteria is failed.
    """
    name = name or 'input range'
    r = dict(first=None, last=None, step=None, unit=None)

    if input_range is None:
        if allow_none:
            return r if todict else None
        else:
            raise ValueError(f'{name} is empty!')

    if not isinstance(input_range, tuple):
        raise TypeError(f'{name} should be a tuple!')

    minlen = 2
    maxlen = 4

    if step is True:
        minlen += 1
    elif step is False:
        maxlen -= 1

    if unit is True:
        minlen += 1
    elif unit is False:
        maxlen -= 1

    if len(input_range) < minlen or len(input_range) > maxlen:
        length = minlen if minlen == maxlen else f'{minlen} to {maxlen}'
        raise TypeError(f'{name} should be of length {length}!')

    r['first'] = input_range[0]
    r['last'] = input_range[1]

    if not isinstance(r['first'], float) or not isinstance(r['last'], float):
        raise TypeError(f'{name} range values should be of type float!')

    if step is not False:
        if step:  # required
            r['step'] = input_range[2]
            if not isinstance(r['step'], float):
                raise TypeError(f'{name} step should be of type float!')
        else:  # optional
            r['step'] = input_range[2] if len(input_range) > minlen else None
            r['step'] = r['step'] if isinstance(r['step'], float) else None
    if r['step']:
        if r['step'] == 0.:
            raise ValueError(f'{name} step cannot be zero!')
        if np.sign(r['last'] - r['first']) != np.sign(r['step']):
            raise ValueError(f'{name} range and step signs should be equal!')
    else:
        if r['last'] <= r['first']:
            raise ValueError(f'{name} range should be incremental!')

    if unit is not False:
        if unit:  # required
            r['unit'] = input_range[-1]
            if not isinstance(r['unit'], str):
                raise TypeError(f'{name} unit should be of type string!')
        else:  # optional
            r['unit'] = input_range[-1] if len(input_range) > minlen else None
            r['unit'] = r['unit'] if isinstance(r['unit'], str) else None

    return r if todict else None
