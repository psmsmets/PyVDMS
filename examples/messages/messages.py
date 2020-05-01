"""
Messages
========

VDMS messages a classes.

"""
from pyvdms import messages

###############################################################################
# Message index
# -------------

# View all implemented messages.

messages.index()

# These are all extensions of :class:`pyvdms.messages.Message`.

###############################################################################
# An first message
# ----------------

# Each message class has its dedicated parameters and its own docstring.
print(messages.Waveform.__init__.__doc__)

# All listed parameters can be set in the message object.

# Create a waveform message
msg = messages.Waveform(starttime='2020-02-02', station='I18*', channel='*')

# View the formatted message
print(msg.message)

# Or simply
print(msg)

# Update message parameters
msg.id = 'demo'
msg.station = 'I55*'
msg.channel = '*F'

# Setting the time period requires the dedicated method
msg.set_time(start='2020-02-02', end=3600.)
print(msg.time)

# View the final message
print(msg)
