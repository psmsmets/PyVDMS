"""
Filesystem
==========

Automatic waveform retrieval for your local SDS archive.

"""
from pyvdms import waveforms2SDS
import os

###############################################################################
# waveforms2SDS
# -------------

# Automatically download waveforms per day and add them to the SDS archive. If
# waveforms for a specifc station and channel already exist then these are
# skipped. If your SDS archive contains gaps then first the status will be
# requested. If no status information is returned and the gap length exceeds
# the ``force_request`` threshold then the entire day will be (re-) downloaded.

os.makedirs('../../data', exist_ok=True)

resp = waveforms2SDS(
    station='I18*',
    channel='*',
    starttime='2020-02-02',
    endtime='2020-02-05',
    sds_root='../../data',
    threshold=60.,
    request_limit='2GB',
    # debug=True,
)

if resp.success:

    if resp.completed:

        print('Request completed.')

    elif resp.quota_exceeded:

        print(f'Quota limit exceeded. Continue from {resp.time} onwards.')

else:

    print(f'An error occurred during the request:\n{resp.error}')
