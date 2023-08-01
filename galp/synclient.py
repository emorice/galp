"""
Synchronous API exposing a limited subsets of functionnalities
"""

import logging
import zmq

from galp.protocol import Protocol, RoutedMessage
from galp.serializer import Serializer
from galp.task_types import TaskName
from galp.messages import Put, Get

class SynClient(Protocol):
    """
    Synchrounous API limited to tasks that cannot run for a long time.
    """

    def __init__(self, endpoint):
        super().__init__('BK', router=False)

        # pylint: disable=no-member
        self.socket = zmq.Context.instance().socket(zmq.DEALER)
        self.socket.connect(endpoint)
        self.serializer = Serializer()


    def __del__(self):
        self.socket.close(linger=1)

    def get_native(self, name: TaskName, timeout=3):
        """
        Synchronously sends a GET and get result
        """
        self.socket.send_multipart(
            self.write_message(
                self.route_message(None,
                    Get(name=name)
                    )
                )
            )

        # We expect a single message in return
        # TODO: we could receive extra messages, as the socket is meant to be
        # reused, they have to be discarded safely
        event = self.socket.poll(timeout=timeout*1000)
        if not event:
            raise TimeoutError
        msg = self.socket.recv_multipart()
        ans = self.on_message(msg)[0]

        if not isinstance(ans, RoutedMessage) and isinstance(ans.body, Put):
            # Unhandled message, should not happen
            logging.warning('Unexpected message')
            return None

        data, children = ans.body.data, ans.body.children

        if children:
            raise NotImplementedError

        return self.serializer.loads(data, [])

    def on_put(self, msg: Put):
        return msg
