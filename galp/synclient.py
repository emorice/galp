"""
Synchronous API exposing a limited subsets of functionnalities
"""

import zmq
import logging

from galp.protocol import Protocol
from galp.serializer import Serializer

class SynClient(Protocol):
    """
    Synchrounous API limited to tasks that cannot run for a long time.
    """

    def __init__(self, endpoint):
       super().__init__()

       self.socket = zmq.Context.instance().socket(zmq.DEALER)
       self.socket.connect(endpoint)
       self.serializer = Serializer()
       self.route = []


    def __delete__(self):
        self.socket.close(linger=1)

    def get_native(self, handle, timeout=3):
        self.get(self.route, handle.name)

        # We expect a single message in return
        # TODO: we could receive extra messages, as the socket is meant to be
        # reused, they have to be discarded safely
        event = self.socket.poll(timeout=timeout*1000)
        if not event:
            raise TimeoutError
        msg = self.socket.recv_multipart()
        ans = self.on_message(msg)

        if ans is False:
            # Unhandled message, should not happen
            logging.warning('Unexpected message')
            return None

        proto, serial, children = ans

        if children:
            raise NotImplementedError

        return self.serializer.loads(handle, proto, serial, [])

    def on_put(self, route, name, proto, serial, children):
       return proto, serial, children

    def send_message(self, routed_msg):
        self.socket.send_multipart(routed_msg)
