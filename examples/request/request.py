"""
Request
=======

VDMS request service.

"""
from pyvdms import Request
from pyvdms.messages import Waveform

###############################################################################
# An example message
# ------------------

# Create a waveform message.
msg = Waveform(starttime='2020-02-02', station='I18*', channel='BDF')

# Inspect the formatted message
print(msg)


###############################################################################
# Initate a request
# -----------------

# Messages are handled as requests.
request = Request(msg)

# The messages to be send
request.message

# Create an empty request
request = Request(None)

# Set (or update) and get the request
request.message = msg


###############################################################################
# Submit a request
# ----------------

# Messages and output files are written to disk in your tmp folder. A new
# folder is created per request and immediately removed after the request is
# completed (also on fail).
result = request.submit()

# The data is returned as Python and user-friendly objects, mainly as
# Obspy objects or a Pandas DataFrame.
fig = result.plot()

# Get the status of the request.
request.status

# Or an overview.
request

# The full logs of the `nms_client` request are wrapped in the object as well.
print(request.logs)

# Re-send the request and only change the station (or any other variable).
request.message.station = 'I37*'
result = request.submit()
fig = result.plot()
