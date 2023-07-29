"""
Broker classes
"""

import logging

from typing import Any
from dataclasses import dataclass

import zmq

from galp.protocol import IllegalRequestError
from galp.reply_protocol import ReplyProtocol
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.task_types import TaskName, Resources, CoreTaskDef
from galp.messages import (task_key, Ready, Role, Route, Put, Done, Exited,
                           Failed)
from galp.serializer import load_model

class Broker:
    """
    Load-balancing client-to-worker and a worker-to-broker loops
    """
    def __init__(self, endpoint, n_cpus):
        self.proto = CommonProtocol('CW', router=True, resources=Resources(cpus=n_cpus))
        self.transport = ZmqAsyncTransport(
            self.proto,
            endpoint, zmq.ROUTER, bind=True)

    async def run(self):
        """
        Runs message loop.

        Must be externally cancelled, typically by a signal handler, as the
        broker never finishes by itself.
        """
        return await self.transport.listen_reply_loop()

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
        self.idle_workers: list[Route] = []

        # Internal routing id indexed by self-identifiers
        self.route_from_peer : dict[str, Any] = {}

        # Total resources
        self.resources = resources

        # Route to a worker spawner
        self.pool: Route | None = None

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

    def on_ready(self, ready: Ready):
        assert not ready.forward

        match ready.role:
            case Role.POOL:
                # When the pool manager joins, record its route so that we can
                # send spawn requests later
                if self.pool is None:
                    self.pool = ready.incoming
                else:
                    assert self.pool == ready.incoming
                return None
            case Role.WORKER:
                # First, update the peer map, we need it to handle kill
                # notifications
                self.route_from_peer[ready.local_id] = ready.incoming

                # Then check for the expected pending mission and send it
                pending_alloc = self.alloc_from_task.pop(ready.mission, None)
                if pending_alloc is None:
                    logging.error('Worker came with unknown mission %s', ready.mission)
                    self.idle_workers.append(ready.incoming)
                    return None

                # Before sending, mark it as affected to this worker so we can
                # handle errors and free resources later
                self.alloc_from_wroute[tuple(ready.incoming)] = pending_alloc

                # in: client, for: the worker
                return (pending_alloc.client_route, ready.incoming), pending_alloc.msg_body

    def mark_worker_available(self, worker_route: Route) -> None:
        """
        Add a worker to the idle list after clearing the current task
        information and freeing resources
        """
        # Get task
        # route needs to be converted from list to tuple to be used as key
        alloc = self.alloc_from_wroute.pop(tuple(worker_route), None)
        if alloc:
            # Free resources
            self.resources += alloc.resources
            # Free worker for reuse
            self.idle_workers.append(worker_route)
        # Else, the free came from an unmetered source, for instance a PUT sent
        # by a client to a worker

    def on_done(self, msg: Done):
        self.mark_worker_available(msg.incoming)

    def on_failed(self, msg: Failed):
        self.mark_worker_available(msg.incoming)

    def on_not_found(self, route, name):
        worker_route, _ = route
        self.mark_worker_available(worker_route)

    def on_found(self, route, task_def):
        worker_route, _ = route
        self.mark_worker_available(worker_route)

    def on_put(self, msg: Put):
        self.mark_worker_available(msg.incoming)

    def on_exited(self, msg: Exited):
        peer = msg.peer
        logging.error("Worker %s exited", peer)

        route = self.route_from_peer.get(peer)
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
        new_route : tuple[list[bytes], list[bytes]] = [], alloc.client_route

        if command_name == b'GET':
            return self.not_found(new_route, TaskName(payload))
        if command_name == b'SUBMIT':
            task_def, err = load_model(CoreTaskDef, payload)
            assert not err
            return Failed.plain_reply(new_route, task_def=task_def)
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
        # specific worker or client, forward as-is.
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

        # Todo: include in incoming msg
        resources = Resources(cpus=1)

        # Drop if we don't have a way to spawn workers
        if not self.pool:
            logging.info('Dropping %s (pool not joined)', verb)
            return None

        # Drop if we don't have any resources left
        if resources > self.resources:
            logging.info('Dropping %s (no resources)', verb)
            return None

        # Allocate. At this point the message can be considered
        # as accepted and queued
        # Todo: therefore, we should already reply to the client now that we're
        # taking the task
        self.resources -= resources
        alloc = Allocation(
                resources=resources,
                client_route=incoming_route,
                msg_body=msg_body
                )

        # If we have idle workers, directly forward to one
        if self.idle_workers:
            worker_route = self.idle_workers.pop()
            logging.debug('Worker available, forwarding %s', verb)

            # Save task info
            self.alloc_from_wroute[tuple(worker_route)] = alloc

            # Incoming: still the incoming client, forward: the worker
            new_route = incoming_route, worker_route

            # We build the message and return it to transport
            return new_route, msg_body

        # Else, spawn a new one, and save the request for when it joins
        # For now spawning just mean forwarding the whole request to the
        # pool
        key = task_key(msg_body)
        self.alloc_from_task[key] = alloc
        new_route = [], self.pool
        return new_route, msg_body
