# Mandatory imports
import pandas as pd
from obspy import UTCDateTime, Stream
from obspy.clients.filesystem.sds import Client as SDS_Client
from humanfriendly import format_size, parse_size


# Relative imports
from ..util.logger import Logger
from ..vdms.request import Request
from ..vdms.messages import Channel, Chan_status, Waveform
from .stream2SDS import stream2SDS


__all__ = ['waveforms2SDS']


def waveforms2SDS(
    station: str, channel: str, starttime: UTCDateTime, endtime: UTCDateTime,
    sds_root: str, verbose: bool = True, debug: bool = False,
    logFile: str = None, verify_archive: bool = True,
    force_request: bool = True, force_request_threshold: int = 300,
    request_limit=None,
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

    starttime : :class:`~obspy.UTCDateTime`
        Set the start time.

    endtime : :class:`~obspy.UTCDateTime`
        Set the end time.

    verbose : `bool`, optional
        Enable verbosity if `True` (default).

    debug : `bool`, optional
        Enable debug mode if `True`. Defaults to `False`.

    logFile : `str`, optional
        If given, logs are written to ``logFile``. Defaults to `None`.

    verify_archive : `bool`, optional
        Verify the local SDS archive consistensty on-the-fly.

    force_request : `bool`, optional
        Force a request if channel status returns `None` or if data is missing.
        The missing data threshold can be set with ``force_request_threshold``.

    force_request_threshold : `float`, optional
        Threshold of missing data per day, in seconds, to trigger a
        (re-) request. Threshold should be within [0, 86400] seconds.

    request_limit : `int`  or `str`, optional
        Limit the total request size, in bytes if `int`. A human readable limit
        can be provided as a string. For example: '2GB'.

    Returns
    -------

    response : :class:`Response`
        Response object with a summary.

    """
    days = int((endtime - starttime) / 86400) + 1

    request_limit = request_limit or 0
    if isinstance(request_limit, int):
        request_limit = request_limit
    elif isinstance(request_limit, str):
        request_limit = parse_size(request_limit)
    else:
        raise TypeError('Request limit should be of type int or str.')

    # init
    req_bytes = 0
    log = Logger(verbose, debug, logFile)
    request = Request(None)
    sds_client = SDS_Client(sds_root, sds_type="D", format="MSEED")

    # log header
    log.heading('Waveforms2SDS', 0)
    log.info('      station = {}'.format(station))
    log.info('      channel = {}'.format(channel))
    log.info('    starttime = {}'.format(starttime.strftime('%Y-%m-%d')))
    log.info('      endtime = {}'.format(endtime.strftime('%Y-%m-%d')))
    log.info('force request = {}'.format(force_request))
    if force_request:
        if force_request_threshold < 0 or force_request_threshold >= 86400:
            raise ValueError('Force request threshold should be a positive '
                             'integer ranging from 0 to 86400')
        log.info('    threshold = {} s'.format(force_request_threshold))

    log.info('     sds_root = {}'.format(sds_client.sds_root))
    log.info('         days = {}'.format(days))
    log.info('request limit = {}'.format(
        format_size(request_limit) if request_limit else 'None'
    ))

    # get stations and channels
    log.heading('Request station and channel metadata', 1)

    request.message = Channel(station, channel)

    log.debug('Request message:')
    log.debug(str(request.message))

    log.debug('Send the request message.')
    inventory = request.submit()

    log.debug('Got an answer.\nThis is the summary.')
    log.debug(str(request))

    log.debug('Returned data:')
    log.debug(str(inventory))

    req_bytes += request.size_bytes

    if not isinstance(inventory, pd.DataFrame):
        return Response(
            success=False, error='No station information returned.'
        )

    if 'off_date' not in inventory:
        inventory['off_date'] = None

    log.info('stations (#{}): {}'.format(
        len(inventory.station.unique()),
        ','.join(list(inventory.station.unique()))
    ))
    log.info('channels (#{}): {}'.format(
        len(inventory.channel.unique()),
        ','.join(list(inventory.channel.unique()))
    ))

    log.heading('Verify sds archive', 1)

    for i in range(days):

        time = starttime + i * 86400
        tstr = time.strftime('%Y-%m-%d')

        log.heading('Verify sds archive for {} (day {})'
                    .format(tstr, time.strftime('%j')), 2)

        status = None
        requested_status = False
        request_stations = []
        request_channels = []

        inventory_at_time = inventory[
            (inventory.on_date >= tstr) & (inventory.off_date < tstr) |
            (inventory.off_date.isnull())
        ]

        toFetch = inventory_at_time.to_dict('records')

        try:

            for item in toFetch:

                avail = sds_client.get_availability_percentage(
                    network='IM',
                    station=item['station'],
                    location='',
                    channel=item['channel'],
                    starttime=time,
                    endtime=time + 86400,
                )

                log.debug('... {}.{}: {}%'.format(
                    item['station'], item['channel'], round(avail[0]*100, 3)
                ))

                doRequest = not (avail[0] > 0)

                if not doRequest:

                    stream = sds_client.get_waveforms(
                        network='IM',
                        station=item['station'],
                        location='',
                        channel=item['channel'],
                        starttime=time,
                        endtime=time + 86400,
                    )

                    if verify_archive:

                        sds_inconsistency = False

                        for trace in stream:

                            trace.stats.sampling_rate = round(
                                trace.stats.sampling_rate, 2
                            )

                            if abs(
                                trace.stats.sampling_rate -
                                item['sampling_rate']
                            ) > 1e-2:

                                log.warning(
                                    ('Trace {}.{} sample rate {} inconsistent '
                                     'with inventory sample rate {}. '
                                     'Removed trace from archive.').format(
                                        item['station'],
                                        item['channel'],
                                        trace.stats.sampling_rate,
                                        item['sampling_rate'],
                                    )
                                )

                                stream.remove(trace)

                                sds_inconsistency = True

                        if sds_inconsistency:

                            stream2SDS(stream, sds_root, verbose=False)

                    npts_in_day = int(item['sampling_rate'] * 86400)

                    npts_in_sds = sum([trace.stats.npts for trace in stream])

                    if npts_in_sds < 10 * item['sampling_rate']:  # one buffer?

                        doRequest = True

                    elif npts_in_sds < npts_in_day:

                        if not requested_status:

                            log.info('... request status')

                            requested_status = True

                            request.message = Chan_status(
                                station=item['station'][0:3] + '*',
                                channel='*',
                                starttime=time,
                            )
                            log.debug('Request message:')
                            log.debug(str(request.message))

                            log.debug('Send the request message.')
                            status = request.submit()

                            log.debug('Got an answer.\n'
                                      'This is the summary.')
                            log.debug(str(request))

                            log.debug('Request data:')
                            log.debug(str(status))

                            req_bytes += request.size_bytes

                            if not isinstance(status, pd.DataFrame):

                                log.warning('Status request returnend None.')

                                if force_request:

                                    log.info('... all waveforms for this day '
                                             'shall be request if threshold '
                                             'exceeded.')

                                else:

                                    log.info('... waveform requests for this '
                                             'day shall be skipped.')

                                    log.debug(request.log)

                                    raise SkipDay

                        npts_in_nms = (
                            int(sum(status[
                                (status.station == item['station']) &
                                (status.channel == item['channel']) &
                                (status.samples > 0)
                            ].samples.values))
                            if isinstance(status, pd.DataFrame) else -1
                        )

                        log.debug(
                            '... {}.{} day:{} sds:{} nms:{}'.format(
                                item['station'],
                                item['channel'],
                                npts_in_day,
                                npts_in_sds,
                                npts_in_nms,
                            )
                        )

                        doRequest = (
                            npts_in_sds < npts_in_nms or
                            (force_request and npts_in_day - npts_in_sds >
                             force_request_threshold*item['sampling_rate'])
                        )

                if doRequest:

                    item['request'] = True

                    if item['channel'] not in request_channels:
                        request_channels.append(item['channel'])

                    if item['station'] not in request_stations:
                        request_stations.append(item['station'])

            if len(request_stations) > 0 and len(request_channels) > 0:

                log.info(
                    '... request missing data:\n'
                    '      station = {}\n'
                    '      channel = {}'
                    .format(
                        ','.join(request_stations),
                        ','.join(request_channels),
                    )
                )

                request.message = Waveform(
                    station=','.join(request_stations),
                    channel=','.join(request_channels),
                    starttime=time,
                )

                log.debug('Request message:')
                log.debug(str(request.message))

                log.debug('Send the request message.')
                stream = request.submit()

                log.debug('Got an answer.\nThis is the summary.')

                log.debug('Request summary:')
                log.debug(str(request))

                log.debug('Request data:')
                log.debug(str(stream))

                req_bytes += request.size_bytes

                if isinstance(stream, Stream):

                    for trace in stream:

                        sr = inventory_at_time[
                            (inventory_at_time.station == trace.stats.station)
                            &
                            (inventory_at_time.channel == trace.stats.channel)
                        ].sampling_rate.values[0]

                        trace.stats.sampling_rate = round(
                            trace.stats.sampling_rate, 2
                        )

                        if abs(trace.stats.sampling_rate - sr) > 1e-2:
                            log.warning('Trace sample rate does not match '
                                        'inventory. Removed trace')
                            stream.remove(trace)

                if stream:

                    stream2SDS(stream, sds_root, verbose=False)

                    log.info('... added stream to archive')

                else:

                    log.info('... no data returned')

                log.info('... request size is {}'
                         .format(request.size))

            else:

                log.info('... nothing to add')

        except SkipDay:

            continue

        except PermissionError:

            log.heading('Waveforms2SDS terminated', 1)

            log.warning("Daily quota reached, you should call it a day.")

            return Response(
                success=True,
                time=time,
                size_bytes=req_bytes,
                quota_exceeded=True,
            )

        except Exception as e:

            log.heading('Waveforms2SDS terminated', 1)

            log.error("An unexpected error occurred.")

            log.error(e)

            return Response(
                success=False,
                time=time,
                size_bytes=req_bytes,
            )

        if request_limit and req_bytes >= request_limit:

            log.heading('Waveforms2SDS terminated', 1)

            log.warning(
                'Total request size of {} exceeds the limit of {}.'
                .format(format_size(req_bytes), format_size(request_limit))
            )

            return Response(
                success=True,
                time=time,
                size_bytes=req_bytes,
                quota_exceeded=False,
            )

    log.heading('Waveforms2SDS terminated', 1)

    log.info('Total request size is {format_size(req_bytes)}.')

    return Response(success=True, size_bytes=req_bytes)


class SkipDay(Exception):
    pass


class Response(object):
    """
    Create a new request response object.

    For details see the :meth:`Response.__init__()`
    method.
    """

    def __init__(
        self, success: bool = False, error: str = None,
        time: UTCDateTime = None, size_bytes: int = None,
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
