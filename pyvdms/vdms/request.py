# -*- coding: utf-8 -*-
"""
Python module for requesting VDMS messages from CTBTO.

.. module:: nms_client

:author:
    Pieter Smets (p.s.m.smets@tudelft.nl)

:copyright:
    Pieter Smets

:license:
    This code is distributed under the terms of the
    GNU General Public License, Version 3
    (https://www.gnu.org/licenses/gpl-3.0.en.html)
"""


# Mandatory imports
from subprocess import Popen, PIPE
import os
from tempfile import mkdtemp
from shutil import rmtree
from pathlib import Path
from tabulate import tabulate
from humanfriendly import format_size


# Relative imports
from ..util.strlist import strlist_contains, strlist_extract
from . import messages


__all__ = ['Request']

_job_error = ' - ERROR - '
_job_status = 'job status has changed to:'
_job_data = 'Save retrieved data'
_job_no_results = 'Your request did not return any results.'


class Request(object):
    """
    Request object.

    For details see the :meth:`~vdms.request.Request.__init__()` method.
    """

    def __init__(self, message, command_line_client: str = None,
                 scratch: str = None, **kwargs):
        """
        Initializes a CTBTO VDMS request.

        >>> request = Request(message)

        :type message: `str` or :class:
        :param message: VDMS request message.
        """
        if message:
            self.set_message(message, **kwargs)
        else:
            self._message = None
            self._reset()
        self._clc = command_line_client or 'nms_client'
        self._test_passed = False

    def __str__(self):
        """Get the formatted VDMS request overview.
        """
        out = []

        out += [['message', self.message.name if self.message else 'None']]
        out += [['status', self.status or 'None']]
        out += [['size', f'{self.size}']]
        out += [['error', f'{self.error}']]
        out += [['command line client', self.clc]]
        out += [['test passed', f'{self._test_passed}']]

        return tabulate(out)

    def _repr_pretty_(self, p, cycle):
        p.text(self.__str__())

    def _reset(self):
        """Reset the request data.
        """
        self._status = None
        self._error = None
        self._stdout = None
        self._num_bytes = None

    @property
    def command_line_client(self):
        """Get the command line client.
        """
        return self._clc

    @property
    def clc(self):
        """Get the command line client.
        """
        return self._clc

    @property
    def message(self):
        """Get or set the request message.
        """
        return self._message

    @message.setter
    def message(self, message):
        if not isinstance(message, messages.Message):
            raise TypeError('message should be a Message class')
        self._message = message
        self._reset()

    def set_message(self, message, **kwargs):
        """
        Set a new request message.

        Parameters
        ----------

        message : `str` or :class:`Message`
            Set the request message by providing an initiated request message
            object or create a new message by the request message name.

        **kwargs :
            Parameters passed to the new request message object, if ``message``
            is a string.

        """
        if isinstance(message, str):
            if message.lower() not in messages.index(lowercase=True):
                raise ValueError(
                    "Illegal request message \"{}\".\nMessages are any of: {}."
                    .format(message, '|'.join(messages.index(lowercase=True)))
                )
            self._message = eval(f'messages.{message.capitalize()}(**kwargs)')
        elif isinstance(message, messages.Message):
            self._message = message
        else:
            raise TypeError('message should be a string or message class')
        self._reset()

    @property
    def error(self):
        """Get the request error.
        """
        return self._error

    @property
    def logs(self):
        """Get the request logs.
        """
        return self._stdout

    @property
    def status(self):
        """Get the request status.
        """
        return self._status.lower() if self._status else None

    @property
    def size_bytes(self):
        """Get the request size in byes.
        """
        return self._num_bytes

    @property
    def size(self):
        """Get the human readable request size.
        """
        return format_size(self._num_bytes) if self._num_bytes else None

    def submit(self, **kwargs):
        """Submit the request message.
        """
        if not isinstance(self._message, messages.Message):
            return

        if not self._test_passed:
            self._test_passed = self._test_request()

        self._status = 'SUBMITTED'
        self._num_bytes = 0

        scratch = Path(mkdtemp())

        request = os.path.join(scratch, f'{self.message.id}.req')
        response = f'{self.message.id}.tmp'
        result = None

        try:

            with open(request, "w") as req:

                req.write(str(self.message))

            process = Popen(
                [self.clc, request, '-f', response, '-d', scratch],
                stdout=PIPE,
                stderr=PIPE,
            )
            process.wait()

            self._stdout = (process.stdout.read()).decode('ascii')
            stdout = self._stdout.splitlines()

            if not strlist_contains(stdout, _job_status):

                if strlist_contains(stdout, 'ERROR_LOG'):
                    self._error = strlist_extract(
                        strlist_extract(stdout, 'ERROR_LOG', offset=1),
                        _job_error
                    )[0]
                else:
                    self._error = strlist_extract(stdout, _job_error)[-2]
                raise PermissionError(self._error)

            self._status = strlist_extract(stdout, _job_status)[-1]
            completed = self._status == 'COMPLETED'

            results = not strlist_contains(stdout, _job_no_results)

            if completed and results:

                if strlist_contains(stdout, _job_data):

                    results = []

                    for f in strlist_extract(stdout, _job_data):

                        results += [(f.split(' in ', 1)[1].strip()[:-2])]

                    result = self.message.handler(results, **kwargs)
            else:

                if strlist_contains(stdout, _job_error):

                    self._error = strlist_extract(stdout, _job_error)[0]

                    if "daily quota" in self._error:
                        raise PermissionError(self._error)

                    else:
                        raise RuntimeError(self._error)

            for retrieved in strlist_extract(stdout, 'Retrieved'):

                self._num_bytes += int(retrieved.split(' ')[0])

        finally:

            rmtree(scratch)

        return result

    def _test_request(self, command_line_client: str = None):
        """
        Test the CTBTO VDMS command line client tool. You can specify the path
        and name by ``command_line_client``.

        Returns `True` if the command line client has been executed
        successfully, otherwise `False`.
        """
        clc = command_line_client or self.command_line_client
        msg = f'Could not test run "{command_line_client} --help"!'
        # test 1: nms_client --help
        try:
            process = Popen([clc, '--help'], stdout=PIPE, stderr=PIPE)
            process.communicate()[0]
            if process.returncode != 0:
                raise RuntimeError(msg)
        except Exception:
            raise RuntimeError(msg + 'Did you set the correct path?')
        # test 2: nms_client test.req
        try:
            process = Popen([clc, '--help'], stdout=PIPE, stderr=PIPE)
            process.communicate()[0]
            if process.returncode != 0:
                raise RuntimeError(msg)
        except Exception:
            raise RuntimeError(msg + 'Did you set the correct path?')
        return True
