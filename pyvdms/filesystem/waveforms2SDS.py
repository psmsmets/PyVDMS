# Mandatory imports
import pandas as pd
from obspy import UTCDateTime, Stream
from obspy.clients.filesystem.sds import Client as SDS_Client
from humanfriendly import format_size, parse_size


# Relative imports
from ..util.time import set_time_range
from ..util.logger import Logger
from ..vdms.request import Request
from ..vdms.messages import Channel, Chan_status, Waveform
from .stream2SDS import stream2SDS


__all__ = ['waveforms2SDS']


def waveforms2SDS(
    station: str, channel: str, starttime, endtime, sds_root: str,
    threshold: float = 300., sds_qc: bool = True, request_limit=None,
    verbose: bool = True, debug: bool = False, log_file: str = None
):
    """Add waveforms to your local SDS archive.

    Parameters:
    -----------

    station : `str`
        Select one or more SEED station codes. Multiple codes are
        comma-separated (e.g. "ANMO,PFO"). Wildcards are allowed.

    channel : `str`
        Select one or more SEED channel codes. Multiple codes are
        comma-separated (e.g. "BHZ,HHZ,*N"). Wildcards are allowed.

    starttime : various
        Set the start time.

    endtime : various
        Set the end time.

    sds_root : `str`
        Path to the local SDS archive.

    sds_qc : `bool`, optional
        Perform a simple quality control of the local SDS archive sample rate
        and correct on-the-fly.

    threshold : `float`, optional
        Force a request if the data gap per day exceeds the threshold, in
        seconds. The threshold should be within [0, 86400] seconds. If `None`,
        or -1, no request is forced. If 0, any sample difference triggers a
        request. Defaults to 300 seconds.

    request_limit : `int`  or `str`, optional
        Limit the total request size, in bytes if `int`. A human readable limit
        can be provided as a string. For example: '2GB'.

    verbose : `bool`, optional
        Enable verbosity if `True` (default).

    debug : `bool`, optional
        Enable debug mode if `True`. Defaults to `False`.

    log_file : `str`, optional
        If given, logs are written to ``log_file``. Defaults to `None`.

    Returns
    -------

    response : :class:`Response`
        Response object with a summary.

    """

    if not isinstance(station, str):
        raise TypeError('station should be of type string.')

    if not isinstance(channel, str):
        raise TypeError('channel should be of type string.')

    # set the time range
    starttime, endtime = set_time_range(starttime, endtime)

    # date range of full days
    days = pd.date_range(
        start=starttime+pd.tseries.offsets.DateOffset(days=0, normalize=True),
        end=endtime+pd.tseries.offsets.DateOffset(days=0, normalize=True),
        freq='1D',
    )

    # threshold
    threshold = threshold or 300.
    threshold = 86400. if threshold == -1. else threshold

    if not isinstance(threshold, float):
        raise TypeError('threshold should be of type float.')

    if threshold < 0. or threshold > 86400.:
        raise ValueError('threshold threshold should be within 0 to 86400')

    # set request limit from bytes or a formatted string
    request_limit = request_limit or 0

    if isinstance(request_limit, int):
        request_limit = request_limit
    elif isinstance(request_limit, str):
        request_limit = parse_size(request_limit)
    else:
        raise TypeError('Request limit should be of type int or str.')

    limit = format_size(request_limit) if request_limit else 'None'

    # init main variables
    log = Logger(verbose, debug, log_file)
    request = Request(None)
    request_size = 0
    sds_client = SDS_Client(sds_root, sds_type="D", format="MSEED")

    # log header and parameters
    log.heading('Waveforms2SDS', 1)
    log.info(f'       station = {station}')
    log.info(f'       channel = {channel}')
    log.info(f'    start time = {days[0]}')
    log.info(f'      end time = {days[-1]}')
    log.info(f'          days = {len(days)}')
    log.info(f'      sds_root = {sds_root}')
    log.info(f'        sds_qc = {sds_qc}')
    log.info(f'     threshold = {threshold}s')
    log.info(f' request limit = {limit}')

    #
    # get stations and channels
    #
    log.info('')
    log.heading('Request station and channel metadata', 2)

    request.message = Channel(station, channel)

    log.debug('Request message:')
    log.debug(str(request.message))

    log.debug('Send the request message.')
    inv = request.submit()

    log.debug('Got an answer.\nThis is the summary.')
    log.debug(str(request))

    log.debug('Returned data:')
    log.debug(str(inv))

    request_size += request.size_bytes

    if not isinstance(inv, pd.DataFrame):
        return Response(
            success=False, error='No station information returned.'
        )

    log.info('stations (#{}): {}'.format(
        len(inv.station.unique()),
        ','.join(list(inv.station.unique()))
    ))
    log.info('channels (#{}): {}'.format(
        len(inv.channel.unique()),
        ','.join(list(inv.channel.unique()))
    ))

    #
    # sds availiability function
    #
    def sds_availability(**kwargs):
        """
        """
        avail = sds_client.get_availability_percentage(**kwargs)

        return avail[0]

    #
    # sds qc function
    #
    def verify_sds_sampling_rate(sampling_rate, **kwargs):
        """
        """
        st = sds_client.get_waveforms(**kwargs)

        inconsistency = False

        for tr in st:

            sampling_error = abs(tr.stats.sampling_rate - sampling_rate)

            if sampling_error > 0.:

                if not inconsistency:
                    log.warning(
                        '{} sample rate {:.3f} in archive inconsistent with '
                        'vdms inventory sample rate {:.3f}.'
                        .format(tr.stats.id, tr.stats.sampling_rate,
                                sampling_rate)
                    )

                if sampling_error > 1e-2:
                    raise RequestItem

                tr.stats.sampling_rate = sampling_rate
                inconsistency = True

        if inconsistency:
            stream2SDS(st, sds_root, method='overwrite', verbose=False)

    try:

        # evaluate per day
        for day in days:

            # check limit
            if request_limit and request_size >= request_limit:
                break

            # get start and end time
            t0 = UTCDateTime(day)
            t1 = t0 + 86400

            # construct base argument dictionary
            day_args = dict(network='IM', location='',
                            starttime=t0, endtime=t1)

            # user feedback
            log.info('')
            log.heading('Verify sds archive for {} (day {})'.format(
                day.strftime('%Y-%m-%d'), day.strftime('%j')), 2
            )

            # init day variables
            day_status = None
            day_status_requested = False
            missing_items = []

            # slice inventory for day
            day_inv = inv[(inv.on_date <= day) &
                          ((inv.off_date > day) | (inv.off_date.isnull()))]

            # loop over items in day inventory
            for item in day_inv.itertuples():

                # catch SkipItem, RequestItem, and any other error
                try:

                    # construct stream argument dictionary
                    sds_args = dict(**day_args, station=item.station,
                                    channel=item.channel)

                    # extra analysis if data availability
                    if sds_qc:
                        verify_sds_sampling_rate(item.sampling_rate,
                                                 **sds_args)

                    # get availability
                    sds_avail = sds_availability(**sds_args)
                    sds_sec = sds_avail * 86400.

                    log.info(
                        f'{item.station}.{item.channel} @ sds  '
                        f'{sds_avail*100:6.2f}% ({86400-sds_sec:.2f}s)'
                    )

                    # go to next item if 100%
                    if sds_avail == 1:
                        raise SkipItem

                    # direct request if no data availability
                    if not sds_avail > 0:
                        raise RequestItem

                    # get availability
                    if not day_status_requested:

                        log.info('Request status for day.')

                        day_status_requested = True

                        request.message = Chan_status(
                            station=item.station[0:3] + '*',
                            channel='*',
                            starttime=day,
                        )

                        day_status = request.submit()

                        request_size += request.size_bytes

                        if not isinstance(day_status, pd.DataFrame):

                            log.warning('Status request returned None.')
                            log.info('All waveforms for this day shall be '
                                     'requested if threshold is exceeded.')

                    if isinstance(day_status, pd.DataFrame):

                        status = day_status.loc[
                            (day_status.station == item.station) &
                            (day_status.channel == item.channel)
                        ]

                        vdms_sec = (status[['samples']].sum().values[0] /
                                    item.sampling_rate)
                        vdms_avail = vdms_sec / 86400

                        log.debug(
                            f'{item.station}.{item.channel} @ vdms '
                            f'{vdms_avail*100:6.2f}% ({86400-vdms_sec:.2f}s)'
                        )

                        if vdms_sec - sds_sec >= threshold:
                            raise RequestItem

                    elif 86400 - sds_sec >= threshold:
                        raise RequestItem

                except SkipItem:
                    continue

                except RequestItem:
                    missing_items.append(item)
                    continue

            # Request missing items for day?
            if len(missing_items) == 0:
                log.info('Nothing to add for this day.')
                continue

            stats = ','.join(set([item.station for item in missing_items]))
            chans = ','.join(set([item.channel for item in missing_items]))

            log.info('Request missing data:')
            log.info(f'       station = {stats}')
            log.info(f'       channel = {chans}')

            request.message = Waveform(station=stats, channel=chans,
                                       starttime=day)

            log.debug('Waveform request message:')
            log.debug(str(request.message))

            st = request.submit()

            log.debug('Got an answer.\nThis is the summary:')
            log.debug(str(request))

            request_size += request.size_bytes

            if isinstance(st, Stream):
                stream2SDS(st, sds_root, method='overwrite', verbose=False)
                log.info('Added stream to archive')

            else:
                log.info('No data returned')

            log.info(f'Request size is {request.size}')

    except PermissionError:

        log.heading('Waveforms2SDS terminated', 1)
        log.warning("Daily quota reached, you should call it a day.")

        return Response(success=True, time=day, size_bytes=request_size,
                        quota_exceeded=True)

    except Exception as e:

        log.heading('Waveforms2SDS terminated', 1)
        log.error("An unexpected error occurred.")
        log.error(e)

        return Response(success=False, time=day, size_bytes=request_size)

    # Completed
    log.heading('Waveforms2SDS terminated', 1)

    if request_limit and request_size >= request_limit:
        log.warning(f'Total request size of {format_size(request_size)} '
                    f'exceeds the limit of {format_size(request_limit)}.')
        response = Response(success=True, time=day, size_bytes=request_size,
                            quota_exceeded=False)

    else:
        log.info(f'Total request size is {format_size(request_size)}.')
        response = Response(success=True, size_bytes=request_size)

    return response


class SkipItem(Exception):
    pass


class RequestItem(Exception):
    pass


class Response(object):
    """
    Create a new request response object.

    For details see the :meth:`Response.__init__()`
    method.
    """

    def __init__(
        self, success: bool = False, error: str = None,
        time: pd.Timestamp = None, size_bytes: int = None,
        quota_exceeded: bool = False
    ):
        """
        Initializes a Client response object.

        >>> response = ClientResponse()

        """
        self._success = success or False
        self._error = error or None
        self._time = time or None
        self._num_bytes = size_bytes or None
        self._quota_exceeded = quota_exceeded or False

    @property
    def success(self):
        """
        Return the response success status.
        """
        return self._success

    @property
    def error(self):
        """
        Return the error.
        """
        return self._error

    @property
    def completed(self):
        """
        Return if the response is completed.
        """
        return self._time is None

    @property
    def time(self):
        """
        Return the response time if not completed.
        """
        return self._time

    @property
    def size_bytes(self):
        """
        Return the total response size in bytes.
        """
        return self._num_bytes

    @property
    def size(self):
        """
        Return the total response size human readable.
        """
        return format_size(self._num_bytes) if self._num_bytes else 'nothing'

    @property
    def quota_exceeded(self):
        """
        Return if the daily quota limit was reached.
        """
        return self._quota_exceeded

    @property
    def quota_remaining(self):
        """
        Return if the daily quota limit was not yet reached.
        """
        return (not self._quota_exceeded)

    @property
    def status(self):
        """
        Return the request status as a human reabable string.
        """
        if not self.success:
            return 'error'
        if self.completed:
            return 'completed'
        else:
            return 'paused'
