r"""

:mod:`util.logger` -- Logger function
========================================

Extend and prepare the loggings.

"""

# Manadatory imports
import sys
import warnings
import logging


# Relative imports
from ..util.headings import heading, hr


__all__ = ['Logger']


class StreamHandler(logging.StreamHandler):
    """
    """
    def __init__(self, arg):
        super().__init__(arg)

    def emit(self, record):
        if isinstance(record.msg, str):
            messages = record.msg.split('\n')
            for message in messages:
                record.msg = message
                super().emit(record)
        else:
            super().emit(record)


class FileHandler(logging.FileHandler):
    """
    """
    def __init__(self, arg):
        super().__init__(arg)

    def emit(self, record):
        if isinstance(record.msg, str):
            messages = record.msg.split('\n')
            for message in messages:
                record.msg = message
                super().emit(record)
        else:
            super().emit(record)


class Logger(logging.Logger):
    """
    """

    def __init__(self, verbose: bool = True, debug: bool = False,
                 logFile: str = None, name: str = None):
        """
        Initate a log object.

        Parameters:
        -----------

        verbose : `bool`, optional
            Verbose logs to stdout. Defaults to `True`.

        debug : `bool`, optional
            Set logging level to DEBUG. Defaults to `False`, only logging INFO.

        logFile : `str`, optional
            Write all logs to file instead of stdout. Defaults to `None`.

        name : `str`, optional
            Set the name of the logger. Defaults to `None`.

        """
        warnings.filterwarnings('default')
        warnings.simplefilter("ignore", ResourceWarning)

        logging.captureWarnings(True)

        name = name or ''

        if not isinstance(name, str):
            raise TypeError('``name`` should be of type `str`.')

        super().__init__(name)

        self.handlers = []

        if verbose:
            self.setLevel(logging.INFO)

        if debug:
            self.setLevel(logging.DEBUG)

        handler = (FileHandler(logFile) if logFile
                   else StreamHandler(sys.stdout))

        formatter = '%(asctime)s - %(levelname)s - %(message)s'
        handler.setFormatter(logging.Formatter(formatter))

        self.addHandler(handler)

    def heading(self, title: str, level):
        """Add a heading to the logs.

        Parameters:
        -----------

        title: `str`
            The heading title.

        level: `int` or `str`
            Level number from 0 to 5, or any of {'part', 'chapter', 'section',
            'subsection', 'subsubsection', 'paragraph'}.
        """
        for line in heading(title, level, True):
            self.info(line)

    def title(self, title: str):
        """Add a title to the logs.
        Short notation for :meth:`self.heading(title, 'section').`
        """
        self.heading(title, 'section')

    def hr(self, **kwargs):
        """Add a horizontal line to the logs.
        """
        self.info(hr(**kwargs))
