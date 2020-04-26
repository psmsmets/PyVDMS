r"""

:mod:`util.headings` -- Heading functions
=========================================

Construct various headings following the Python’s Style Guide for documenting.

    # with overline, for parts
    * with overline, for chapters
    =, for sections
    -, for subsections
    ^, for subsubsections
    ", for paragraphs

"""


__all__ = ['heading', 'hr']

_heading_levels = ['part', 'chapter', 'section', 'subsection',
                   'subsubsection', 'paragraph']


def heading(title: str, level, to_list: bool = False):
    """Construct a heading.

    Parameters:
    -----------

    title: `str`
        The heading title.

    level: `int` or `str`
        Level number from 0 to 5, or any of {'part', 'chapter', 'section',
        'subsection', 'subsubsection', 'paragraph'}.
    """

    if isinstance(level, int):
        if level < 0 or level >= len(_heading_levels):
            raise ValueError('Heading level number should be in [0, 5].')
        level = _heading_levels[level]
    elif isinstance(level, str):
        if level not in _heading_levels:
            raise KeyError('Heading level name should be any of {}'
                           .format('|'.join(_heading_levels)))
    else:
        raise TypeError('Level should be of type `int` or `str`!')

    return eval(f'heading_{level}(title, to_list)')


def hr(linelen: int = 65, sym: str = '*', fill: bool = None):
    """Construct a horizontal line.
    """
    fill = fill or sym
    return '{s}{f}{s}'.format(s=sym, f=''.join(fill*(linelen-2)))


def heading_part(title: str, to_list: bool = False):
    """Construct a part heading
    """
    line = hr(len(title), '#')
    linelist = [line, title, line]
    return linelist if to_list else '\n'.join(linelist)


def heading_chapter(title: str, to_list: bool = False):
    """Construct a chapter heading.
    """
    line = hr(len(title), '*')
    linelist = [line, title, line]
    return linelist if to_list else '\n'.join(linelist)


def heading_section(title: str, to_list: bool = False):
    """Construct a section heading.
    """
    linelist = [title, hr(len(title), '=')]
    return linelist if to_list else '\n'.join(linelist)


def heading_subsection(title: str, to_list: bool = False):
    """Construct a subsection heading.
    """
    linelist = [title, hr(len(title), '-')]
    return linelist if to_list else '\n'.join(linelist)


def heading_subsubsection(title: str, to_list: bool = False):
    """Construct a subsubsection heading.
    """
    linelist = [title, hr(len(title), '^')]
    return linelist if to_list else '\n'.join(linelist)


def heading_paragraph(title: str, to_list: bool = False):
    """Construct a paragraph heading.
    """
    linelist = [title, hr(len(title), '"')]
    return linelist if to_list else '\n'.join(linelist)
