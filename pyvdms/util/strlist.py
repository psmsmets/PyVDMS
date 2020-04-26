r"""

:mod:`util.strlist` -- String lists
===================================

Common string list methods.

"""

__all__ = ['strlist_contains', 'strlist_extract']


def strlist_contains(lines, substr: str, case_sensitive: bool = True):
    """
    Check if a string list contains a specific substr.
    """

    sublist = strlist_extract(lines, substr, case_sensitive=case_sensitive,
                              split=False, offset=0)

    return len(sublist) > 0


def strlist_extract(lines, substr: str, split: bool = True,
                    offset: int = None, case_sensitive: bool = True):
    """
    Extract a specific substr from a list of strings.
    If ``split`` is `True` (default), the line is split on `substr` and the
    remaining string after `substr` is returned. When ``split`` is `False`
    each line that contains ``substr`` is returned.
    """

    lines = lines or []

    if not isinstance(lines, list):
        raise TypeError('lines should be a list of strings.')

    if not lines:
        return []

    if not isinstance(substr, str):
        raise TypeError('substr should be a string.')

    offset = offset or 0

    if not isinstance(offset, int):
        raise TypeError('line offset should be an integer.')

    if abs(offset) >= len(lines):
        raise ValueError('line offset should be smaller than number of lines.')

    split = split if offset == 0 else False

    sublist = []

    first = -offset if offset < 0 else 0
    last = len(lines) - (offset if offset > 0 else 0)

    if not case_sensitive:
        substr = substr.lower()

    for i, line in enumerate(lines[first:last], start=first):

        if not isinstance(line, str):
            raise TypeError('lines should all be strings!')

        if substr in (line if case_sensitive else line.lower()):
            sublist.append(line.split(substr, 1)[1].strip() if split
                           else lines[i+offset])

    return sublist
