"""
Broker classes
"""

import logging

from typing import Any
from dataclasses import dataclass
from collections import defaultdict
from itertools import cycle

import zmq

import galp.messages as gm
import galp.task_types as gtt

from galp.protocol import Route, RoutedMessage
from galp.reply_protocol import ReplyProtocol
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.task_types import Resources

class Broker: # pylint: disable=too-few-public-methods # Compat and consistency
    """
    Load-balancing client-to-worker and a worker-to-broker loops
    """
    def __init__(self, endpoint, n_cpus):
        self.proto = CommonProtocol('CW', router=True, max_cpus=n_cpus)
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
    claim: gtt.ResourceClaim
    resources: Resources
    msg: RoutedMessage
    task_id: bytes

class CommonProtocol(ReplyProtocol):
    """
    Handler for messages received from clients, pool and workers.
    """
    def __init__(self, name: str, router: bool, max_cpus: int):
        super().__init__(name, router)

        # List of idle workers, by resources
        self.idle_workers: defaultdict[gtt.ResourceClaim, list[Route]]
        self.idle_workers = defaultdict(lambda : [])

        # Internal routing id indexed by self-identifiers
        self.route_from_peer : dict[str, Any] = {}

        # Total resources
        self.max_cpus = max_cpus
        self.resources = Resources(cpus=[]) # Available cpus

        # Route to a worker spawner
        self.pool: Route | None = None

        # Tasks currenty accepted or ruuning, by unique id
        self.alloc_from_task: dict[bytes, Allocation] = {}

        # Tasks currently running on a worker
        self.alloc_from_wroute: dict[Any, Allocation] = {}

    def on_routed_ready(self, rmsg: RoutedMessage, gmsg: gm.Ready):
        """
        Register worker route and forward initial worker mission
        """
        assert not rmsg.forward

        # First, update the peer map, we need it to handle kill
        # notifications
        self.route_from_peer[gmsg.local_id] = rmsg.incoming

        # Then check for the expected pending mission and send it
        pending_alloc = self.alloc_from_task.get(gmsg.mission, None)
        if pending_alloc is None:
            logging.error('Worker came with unknown mission %s', gmsg.mission)
            return None

        # Before sending, mark it as affected to this worker so we can
        # handle errors and free resources later
        self.alloc_from_wroute[tuple(rmsg.incoming)] = pending_alloc

        # Fill in the worker route in original request
        return RoutedMessage(
                incoming=pending_alloc.msg.incoming,
                forward=rmsg.incoming,
                body=pending_alloc.msg.body
                )

    def on_routed_pool_ready(self, rmsg: RoutedMessage, gmsg: gm.PoolReady):
        """
        Register pool route and resources
        """
        assert not rmsg.forward
        assert self.pool is None

        # When the pool manager joins, record its route and set the available
        # resources
        self.pool = rmsg.incoming
        # Adapt the list of cpus to the requested number of max cpus by dropping
        # or repeting some as needed
        cpus = [x for x, _ in zip(cycle(gmsg.cpus), range(self.max_cpus))]
        self.resources = Resources(cpus)

    def free_resources(self, worker_route: Route, reuse=True) -> None:
        """
        Add a worker to the idle list after clearing the current task
        information and freeing resources
        """
        # Get task
        # route needs to be converted from list to tuple to be used as key
        alloc = self.alloc_from_wroute.pop(tuple(worker_route), None)
        if alloc:
            # Free resources
            self.resources = self.resources.free(alloc.resources)
            if self.alloc_from_task.pop(alloc.task_id, None) is None:
                logging.error('Double free of allocation %s', alloc)
            # Free worker for reuse if marked as such
            if reuse:
                self.idle_workers[alloc.claim].append(worker_route)
        # Else, the free came from an unmetered source, for instance a PUT sent
        # by a client to a worker

    def on_exited(self, msg: gm.Exited):
        """
        Propagate failuer messages and free resources when worker is killed
        """
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

        # Free resources, since a dead worker uses none, but of course don't
        # mark the worker as reusable
        self.free_resources(route, reuse=False)

        # Note that we set the incoming to empty, which equals re-interpreting
        # the message as addressed to us
        orig_msg = alloc.msg

        match orig_msg.body:
            case gm.Get() | gm.Stat():
                return RoutedMessage(
                        incoming=[],
                        forward=alloc.msg.incoming,
                        body=gm.NotFound(name=orig_msg.body.name)
                        )
            case gm.Exec():
                return RoutedMessage(
                        incoming=[],
                        forward=alloc.msg.incoming,
                        body=gm.Failed(task_def=orig_msg.body.submit.task_def)
                        )
        logging.error(
            'Worker %s died while handling %s, no error propagation',
            peer, alloc
            )
        return None

    def calc_resource_claim(self, msg: RoutedMessage) -> gtt.ResourceClaim:
        """
        Determine resources requested by a request
        """
        if isinstance(msg.body, gm.Submit):
            return msg.body.resources
        return gtt.ResourceClaim(cpus=1)

    def on_request(self, msg: RoutedMessage, gmsg: gm.Stat | gm.Submit | gm.Get):
        """
        Assign a worker
        """
        verb = gmsg.verb

        claim = self.calc_resource_claim(msg)

        # Drop if we already accepted the same task
        # This ideally should not happen if the client receives proper feedback
        # on when to re-submit, but should happen from time to time under normal
        # operation
        task_id = gmsg.task_key
        if task_id in self.alloc_from_task:
            logging.info('Dropping %s (already allocated)', verb)
            return None

        # Drop if we don't have a way to spawn workers
        if not self.pool:
            logging.info('Dropping %s (pool not joined)', verb)
            return None

        # Try allocate and drop if we don't have any resources left
        resources, self.resources = self.resources.allocate(claim)
        if resources is None:
            logging.info('Dropping %s (no resources)', verb)
            return None

        # Allocated. At this point the message can be considered
        # as accepted and queued

        # For Submits, the worker will need to know the details of the
        # allocation, so wrap the original message
        if isinstance(gmsg, gm.Submit):
            msg = RoutedMessage(
                    incoming=msg.incoming, forward=msg.forward,
                    body=gm.Exec(submit=gmsg, resources=resources)
                    )

        # Related to issue #87: we should already reply to the client now that
        # we're taking the task
        alloc = Allocation(
                claim=claim,
                resources=resources,
                msg=msg,
                task_id=task_id
                )
        self.alloc_from_task[alloc.task_id] = alloc


        # If we have idle workers, directly forward to one
        if (workers := self.idle_workers[claim]):
            worker_route = workers.pop()
            logging.debug('Worker available, forwarding %s', verb)

            # Save task info
            self.alloc_from_wroute[tuple(worker_route)] = alloc

            # We build the message and return it to transport
            return RoutedMessage(
                    incoming=msg.incoming,
                    forward=worker_route,
                    body=msg.body
                    )

        # Else, ask the pool to fork a new worker
        # We'll send the request to the worker when it send us a READY
        return RoutedMessage(
                incoming=Route(),
                forward=self.pool,
                body=gm.Fork(alloc.task_id, alloc.claim)
                )

    def on_routed_message(self, msg: RoutedMessage):
        # First, call local handlers. We do that even for messages that we
        # forward as is, as we have some state to update. This also ensures that
        # the message is valid
        gmsg = msg.body

        # Record joining peers and forward pre allocated requests
        if isinstance(gmsg, gm.Ready):
            return self.on_routed_ready(msg, gmsg)

        if isinstance(gmsg, gm.PoolReady):
            return self.on_routed_pool_ready(msg, gmsg)

        # Similarly, record dead peers and forward failures
        if isinstance(gmsg, gm.Exited):
            return self.on_exited(gmsg)

        # Free resources for all messages indicating end of task
        if isinstance(gmsg,
                gm.Done | gm.Failed | gm.NotFound | gm.Found | gm.Put):
            self.free_resources(msg.incoming)

        # If a forward route is already present, the message is addressed at one
        # specific worker or client, forward as-is.
        if msg.forward:
            logging.debug('Forwarding %s', gmsg.verb)
            return msg

        # Else, we may have to forward or queue the message. We decide based on
        # the verb whether this should be ultimately sent to a worker
        if isinstance(gmsg, gm.Stat | gm.Submit | gm.Get):
            return self.on_request(msg, gmsg)

        # If we reach this point, we received a message we know nothing about
        return self.on_unhandled(gmsg)
