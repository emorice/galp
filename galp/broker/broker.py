"""
Broker classes
"""

import asyncio
import logging

from typing import Any
from dataclasses import dataclass

import zmq

from galp.protocol import IllegalRequestError
from galp.reply_protocol import ReplyProtocol
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.task_types import TaskName, NamedTaskDef, Resources
from galp.messages import task_key, Ready, Role

class Broker:
    """
    Load-balancing client-to-worker and a worker-to-broker loops
    """
    def __init__(self, endpoint, n_cpus):
        self.proto = CommonProtocol('CW', router=True, resources=Resources(cpus=n_cpus))
        self.transport = ZmqAsyncTransport(
            self.proto,
            endpoint, zmq.ROUTER, bind=True)

    async def listen_forward_loop(self, transport):
        """Client-side message processing loop of the broker"""

        proto_name = transport.protocol.proto_name
        logging.info("Broker listening for %s on %s", proto_name,
            transport.endpoint
            )

        while True:
            try:
                replies = await transport.recv_message()
            except IllegalRequestError as err:
                logging.error('Bad %s request: %s', proto_name, err.reason)
                await transport.send_message(
                    transport.proto.illegal(err.route)
                    )
            await transport.send_messages(replies)

    async def listen(self):
        """Listen client-side, forward to workers"""
        return await self.listen_forward_loop(self.transport)

    async def run(self):
        """
        Runs both loops.

        Must be externally cancelled
        """
        return await asyncio.gather(
            self.listen(),
            )

@dataclass
class Allocation:
    """
    A request that was accepted, but is not yet treated
    """
    resources: Resources
    client_route: Any
    msg_body: list[bytes]

class CommonProtocol(ReplyProtocol):
    """
    Handler for messages received from workers.
    """
    def __init__(self, name: str, router: bool, resources: Resources):
        super().__init__(name, router)

        # List of idle workers
        self.idle_workers: list[bytes] = []

        # Internal routing id indexed by self-identifiers
        self.route_from_peer : dict[str, Any] = {}

        # Total resources
        self.resources = resources

        # Route to a worker spawner
        self.pool = None

        # verb + name -> (in_route, msg)
        self.alloc_from_task: dict[bytes, Allocation] = {}
        self.alloc_from_wroute: dict[Any, Allocation] = {}

    def on_invalid(self, route, reason):
        raise IllegalRequestError(route, reason)

    def on_unhandled(self, verb):
        """
        For the broker, many messages will just be forwarded and no handler is
        needed.
        """
        logging.debug("No broker action for %s", verb.decode('ascii'))


    def on_ready(self, route, ready_info: Ready):
        incoming, forward = route
        assert not forward

        match ready_info.role:
            case Role.POOL:
                if self.pool is None:
                    self.pool = incoming
                else:
                    assert self.pool == incoming
                return None
            case Role.WORKER:
                # First, update the peer map, we need it to handle kill
                # notifications
                self.route_from_peer[ready_info.local_id] = incoming

                # Then check for the expected pending mission and send it
                pending_alloc = self.alloc_from_task.pop(ready_info.mission, None)
                if pending_alloc is None:
                    logging.error('Worker came with unknown mission %s', ready_info.mission)
                    return None

                # Before sending, mark it as affected to this worker so we can
                # handle errors and free resources later
                self.alloc_from_wroute[tuple(incoming)] = pending_alloc

                # in: client, for: the worker
                return (pending_alloc.client_route, incoming), pending_alloc.msg_body
            #self.idle_workers.append(incoming)

    def mark_worker_available(self, worker_route):
        """
        Add a worker to the idle list after clearing the current task
        information and freeing resources
        """
        # route need to be converted from list to tuple to be used as key
        key = tuple(worker_route)

        # Get task
        alloc = self.alloc_from_wroute.pop(key, None)
        if alloc:
            # Free resources
            self.resources += alloc.resources
            # Free worker for reuse
            self.idle_workers.append(worker_route)
        # Else, the free came from an unmetered source, for instance a PUT sent
        # by a client to a worker

    def on_done(self, route, named_def: NamedTaskDef, children: list[TaskName]):
        worker_route, _ = route
        self.mark_worker_available(worker_route)

    def on_failed(self, route, name):
        worker_route, _ = route
        self.mark_worker_available(worker_route)

    def on_not_found(self, route, name):
        worker_route, _ = route
        self.mark_worker_available(worker_route)

    def on_found(self, route, task_dict):
        worker_route, _ = route
        self.mark_worker_available(worker_route)

    def on_put(self, route, name, serialized):
        worker_route, _ = route
        self.mark_worker_available(worker_route)

    def on_exited(self, route, peer: bytes):
        logging.error("Worker %s exited", peer)

        route = self.route_from_peer.get(peer.decode('ascii'))
        if route is None:
            logging.error("Worker %s is unknown, ignoring exit", peer)
            return None

        alloc = self.alloc_from_wroute.pop(tuple(route), None)
        if alloc is None:
            logging.error("Worker %s was not assigned a task, ignoring exit", peer)
            return None

        # Hacky, stat + name or get + name or sub + payload
        command_name, payload = alloc.msg_body[:2]
        # Normally the other route part is the worker route, but here the
        # worker died so we set it to empty to make sure the client cannot
        # accidentally try to re-use it.
        new_route = [], alloc.client_route

        if command_name == b'GET':
            return self.not_found(new_route, payload)
        if command_name == b'SUBMIT':
            return self.failed_raw(new_route, payload)
        logging.error(
            'Worker %s died while handling %s, no error propagation',
            peer, alloc
            )
        return None

    def on_verb(self, route, msg_body: list[bytes]):
        incoming_route, forward_route = route

        # First, call local handlers. We do that even for messages that we
        # forward as is, as we have some state to update. This also ensures that
        # the message is valid
        replies = super().on_verb(route, msg_body)

        # If any handler returned an answer, we stop forwarding here and send it
        if replies:
            return replies

        verb = msg_body[0].decode('ascii')
        # If a forward route is already present, the message is addressed at one
        # specific worker, forward as-is.
        if forward_route:
            logging.debug('Forwarding %s', verb)
            return route, msg_body

        # Else, we may have to forward or queue the message. We decide based on
        # the verb whether this should be ultimately sent to a worker
        if verb not in ('STAT', 'GET', 'SUBMIT'):
            # Nothing else to do
            return None

        # Finally, assign a worker if we still have something to process at this
        # point

        # TODO: we need to decouple resources and available workers:
        #  * We should track resources and proceed if we have some available.
        #  These are abstract resource limits, not concrete workers
        #  * Once we decided to allocate resources, we should procure a worker
        #  with access to them.
        #
        # else:
        #  drop, or return busy
        #
        # on worker join (key)
        #  get stored msg (key)
        #  forward(msg, worker)

        # |Pseudo code
        # |res = extract_from(msg)
        # Todo: include in incoming msg
        resources = Resources(cpus=1)

        # |if available(res):
        if not self.pool:
            logging.info('Dropping %s (pool not joined)', verb)
            return None
        if resources <= self.resources:
        #  |atomically allocate(res)
            self.resources -= resources
            alloc = Allocation(
                    resources=resources,
                    client_route=incoming_route,
                    msg_body=msg_body
                    )
            # Try re-using workers
            if self.idle_workers:
                worker_route = self.idle_workers.pop()
                logging.debug('Worker available, forwarding %s', verb)

                # Save task info
                self.alloc_from_wroute[tuple(worker_route)] = alloc
                # We save the up to two components of the message body for forensics
                # if the worker dies, along with the client route
                #self.task_from_route[tuple(worker_route)] = (msg_body[:2], incoming_route)

                # Incoming: still the incoming client, forward: the worker
                new_route = incoming_route, worker_route

                # We build the message and return it to transport
                return new_route, msg_body

            # Else, spawn a new one
            # For now we send the original request, but it should include the
            # resource claim too
            #  |store msg, get key
            key = task_key(msg_body)
            self.alloc_from_task[key] = alloc
            #  |spawn_worker(res, msg?)
            # Incoming: us, forward: pool
            new_route = [], self.pool
            return new_route, msg_body

        # Finally, with no route and no worker available, we drop the message
        logging.info('Dropping %s (no resources)', verb)
        return None
