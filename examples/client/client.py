"""
Client
======

A all-in-one VDMS service.

"""
from pyvdms import Client

###############################################################################
# Full-service
# ------------

# A client object wrapping the various VDMS messages and request as dedicated
# functions.

# Initate a client
client = Client()

# DataFrame of all active *H1.BDF stations
ch = client.get_channels(station='*L1', channel='BDF', starttime='yesterday')
print(ch)

# Inventory of all *H1.BDF stations
inv = client.get_stations(station='*L1', channel='BDF')
print(inv)
fig = inv.plot('global')

# Get the status of these stations as a DataFrame (for the entire day)
stat = client.get_status(station='*L1', channel='BDF', starttime='yesterday')
print(stat)

# Waveforms for the first minute of the day.
st = client.get_waveforms(station='*L1', channel='BDF', starttime='yesterday',
                          endtime=60.)
fig = st.plot()

# If something goes wrong you can always inspect the last request object.
print(client.last_request)
