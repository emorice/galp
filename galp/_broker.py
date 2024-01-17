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

from galp.protocol import (UpperSession,
    make_stack, ReplyFromSession, ForwardSessions, TransportMessage)
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.task_types import Resources
from galp.req_rep import ReplySession

class Broker: # pylint: disable=too-few-public-methods # Compat and consistency
    """
    Load-balancing client-to-worker and a worker-to-broker loops
    """
    def __init__(self, endpoint: str, n_cpus: int) -> None:
        self.proto = CommonProtocol(max_cpus=n_cpus)
        stack = make_stack(self.proto.on_local,
                name='CW', router=True,
                on_forward=self.proto.on_forward)
        self.transport = ZmqAsyncTransport(
            stack, endpoint, zmq.ROUTER, bind=True)

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
    msg: gm.Message
    client: ReplyFromSession
    task_id: bytes

class CommonProtocol:
    """
    Handler for messages received from clients, pool and workers.
    """
    def __init__(self, max_cpus: int):
        # List of idle workers, by resources
        self.idle_workers: defaultdict[gtt.ResourceClaim, list[ReplyFromSession]]
        self.idle_workers = defaultdict(lambda : [])

        # Internal routing id indexed by self-identifiers
        self.session_from_peer : dict[str, ReplyFromSession] = {}

        # Total resources
        self.max_cpus = max_cpus
        self.resources = Resources(cpus=[]) # Available cpus

        # Route to a worker spawner
        self.pool: UpperSession | None = None

        # Tasks currenty accepted or ruuning, by unique id
        self.alloc_from_task: dict[bytes, Allocation] = {}

        # Tasks currently running on a worker
        self.alloc_from_wuid: dict[Any, Allocation] = {}

    def on_ready(self, session: ReplyFromSession, msg: gm.Ready):
        """
        Register worker route and forward initial worker mission
        """
        # First, update the peer map, we need it to handle kill
        # notifications
        self.session_from_peer[msg.local_id] = session

        # Then check for the expected pending mission and send it
        pending_alloc = self.alloc_from_task.get(msg.mission, None)
        if pending_alloc is None:
            logging.error('Worker came with unknown mission %s', msg.mission)
            return None

        # Before sending, mark it as affected to this worker so we can
        # handle errors and free resources later
        self.alloc_from_wuid[session.uid] = pending_alloc

        # Fill in the worker route in original request
        return [session
                .reply_from(pending_alloc.client)
                .write(pending_alloc.msg)
                ]

    def on_pool_ready(self, pool: ReplyFromSession, msg: gm.PoolReady):
        """
        Register pool route and resources
        """
        assert self.pool is None

        # When the pool manager joins, record its route and set the available
        # resources
        # We already set the forward-address to None since we will never need to
        # forward anything to pool.
        self.pool = pool.reply_from(None)
        # Adapt the list of cpus to the requested number of max cpus by dropping
        # or repeting some as needed
        cpus = [x for x, _ in zip(cycle(msg.cpus), range(self.max_cpus))]
        self.resources = Resources(cpus)

    def free_resources(self, session: ReplyFromSession, reuse=True) -> None:
        """
        Add a worker to the idle list after clearing the current task
        information and freeing resources
        """
        # Get task
        alloc = self.alloc_from_wuid.pop(session.uid, None)
        if alloc:
            # Free resources
            self.resources = self.resources.free(alloc.resources)
            if self.alloc_from_task.pop(alloc.task_id, None) is None:
                logging.error('Double free of allocation %s', alloc)
            # Free worker for reuse if marked as such
            if reuse:
                self.idle_workers[alloc.claim].append(session)
        # Else, the free came from an unmetered source, for instance a PUT sent
        # by a client to a worker

    def on_exited(self, msg: gm.Exited):
        """
        Propagate failuer messages and free resources when worker is killed
        """
        peer = msg.peer
        logging.error("Worker %s exited", peer)

        session = self.session_from_peer.get(peer)
        if session is None:
            logging.error("Worker %s is unknown, ignoring exit", peer)
            return None

        alloc = self.alloc_from_wuid.pop(session.uid, None)
        if alloc is None:
            logging.error("Worker %s was not assigned a task, ignoring exit", peer)
            return None

        # Free resources, since a dead worker uses none, but of course don't
        # mark the worker as reusable
        self.free_resources(session, reuse=False)

        # Note that we set the incoming to empty, which equals re-interpreting
        # the message as addressed to us
        chan = alloc.client.reply_from(None)
        orig_msg = alloc.msg

        match orig_msg:
            case gm.Get() | gm.Stat():
                return [
                        ReplySession(chan, orig_msg.verb)
                        .write(gm.NotFound(name=orig_msg.name))
                        ]
            case gm.Exec():
                return [
                        ReplySession(chan, orig_msg.submit.verb)
                        .write(gm.Failed(task_def=orig_msg.submit.task_def)
                        )]
        logging.error(
            'Worker %s died while handling %s, no error propagation',
            peer, alloc
            )
        return None

    def calc_resource_claim(self, msg: gm.Message) -> gtt.ResourceClaim:
        """
        Determine resources requested by a request
        """
        if isinstance(msg, gm.Submit):
            return msg.resources
        return gtt.ResourceClaim(cpus=1)

    def on_request(self, session: ReplyFromSession, msg: gm.Stat | gm.Submit | gm.Get):
        """
        Assign a worker
        """
        verb = msg.verb

        claim = self.calc_resource_claim(msg)

        # Drop if we already accepted the same task
        # This ideally should not happen if the client receives proper feedback
        # on when to re-submit, but should happen from time to time under normal
        # operation
        task_id = msg.task_key
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
        new_msg: gm.Message
        if isinstance(msg, gm.Submit):
            new_msg = gm.Exec(submit=msg, resources=resources)
        else:
            new_msg = msg
        del msg

        # Related to issue #87: we should already reply to the client now that
        # we're taking the task
        alloc = Allocation(
                claim=claim,
                resources=resources,
                msg=new_msg,
                client=session,
                task_id=task_id
                )
        self.alloc_from_task[alloc.task_id] = alloc


        # If we have idle workers, directly forward to one
        if (workers := self.idle_workers[claim]):
            worker = workers.pop()
            logging.debug('Worker available, forwarding %s', verb)

            # Save task info
            self.alloc_from_wuid[worker.uid] = alloc

            # We build the message and return it to transport
            return [worker
                    .reply_from(session)
                    .write(new_msg)
                    ]

        # Else, ask the pool to fork a new worker
        # We'll send the request to the worker when it send us a READY
        return [self.pool.write(gm.Fork(alloc.task_id, alloc.claim))]

    def on_forward(self, sessions: ForwardSessions, msg: gm.Message
            ) -> list[TransportMessage]:
        """
        Handles only messages forwarded through broker
        """
        # Free resources for all messages indicating end of task
        if isinstance(msg, gm.Reply):
            if not isinstance(msg.value, gm.Doing):
                self.free_resources(sessions.origin)
        # Forward as-is.
        logging.debug('Forwarding %s', msg.verb)
        return [sessions.dest.reply_from(sessions.origin).write(msg)]

    def on_local(self, session: ReplyFromSession, msg: gm.Message
            ) -> list[TransportMessage]:
        """
        Handles local messages received by the broker
        """
        # Record joining peers and forward pre allocated requests
        if isinstance(msg, gm.Ready):
            return self.on_ready(session, msg)

        if isinstance(msg, gm.PoolReady):
            return self.on_pool_ready(session, msg)

        # Similarly, record dead peers and forward failures
        if isinstance(msg, gm.Exited):
            return self.on_exited(msg)

        # Else, we may have to forward or queue the message. We decide based on
        # the verb whether this should be ultimately sent to a worker
        if isinstance(msg, gm.Stat | gm.Submit | gm.Get):
            return self.on_request(session, msg)

        # If we reach this point, we received a message we know nothing about
        logging.error('Unknown message %s', msg)
        return []
