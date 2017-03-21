from posttroll.publisher import Publish
from posttroll.message import Message


def send_message(topic, msg_type, msg_data, nameservers=None, port=0):
    """Send monitoring message"""
    if nameservers is None:
        nameservers = []
    if not isinstance(nameservers, list):
        nameservers = [nameservers]

    with Publish("pytroll-collectors", port=port,
                 nameservers=nameservers) as pub:
        msg = Message(topic, msg_type, msg_data)
        pub.send(str(msg))
        return str(msg)
