"""
Broker classes
"""

import logging

from dataclasses import dataclass
from collections import defaultdict
from itertools import cycle
from functools import singledispatchmethod

from galp.result import Result
import galp.net.core.types as gm
from galp.net.core.dump import add_request_id, Writer, get_request_id
from galp.net.routing.dump import SessionUid
import galp.task_types as gtt

from galp.protocol import make_forward_handler, ReplyFromSession, TransportMessage
from galp.socket_transport import serve

async def broker_serve(endpoint: str, n_cpus: int) -> Result[object]:
    """
    Load-balancing client-to-worker and a worker-to-broker loops

    Must be externally cancelled, typically by a signal handler, as the
    broker never finishes by itself except on errors.
    """
    return await serve(
            endpoint,
            make_forward_handler(
                CommonProtocol(max_cpus=n_cpus).on_message,
                name='CW'),
            )

@dataclass
class Allocation:
    """
    A request that was accepted, but is not yet treated
    """
    claim: gtt.Resources
    resources: gtt.Resources
    msg: gm.Request
    client: ReplyFromSession
    task_id: bytes


def allocate_resources(avail: gtt.Resources, claim: gtt.Resources
        ) -> tuple[gtt.Resources, gtt.Resources | None]:
    """
    Try to split resources specified by claim off a resource set

    Returns:
        tuple (rest, allocated). If resources are insufficient, `rest` is
        unchanged and `allocated` will be None.
    """
    if claim.cpus > avail.cpus:
        return avail, None

    alloc_cpus, rest_cpus = avail.cpu_list[:claim.cpus], avail.cpu_list[claim.cpus:]

    return (
            gtt.Resources(avail.cpus - claim.cpus, '', tuple(rest_cpus)),
            gtt.Resources(claim.cpus, claim.vm, tuple(alloc_cpus)),
            )

def free_resources(avail: gtt.Resources, alloc: gtt.Resources) -> gtt.Resources:
    """
    Return allocated resources
    """
    return gtt.Resources(avail.cpus + alloc.cpus, '', avail.cpu_list + alloc.cpu_list)

class CommonProtocol:
    """
    Handler for messages received from clients, pool and workers.
    """
    def __init__(self, max_cpus: int):
        # List of idle workers, by resources
        self.idle_workers: defaultdict[gtt.Resources, list[ReplyFromSession]]
        self.idle_workers = defaultdict(lambda : [])

        # Internal routing id indexed by self-identifiers
        self.session_from_peer : dict[str, ReplyFromSession] = {}

        # Total resources
        self.max_cpus = max_cpus
        self.resources = gtt.Resources(cpus=0, vm='') # Available cpus

        # Route to a worker spawner
        self.write_pool: Writer[gm.Message] | None = None

        # Tasks currenty accepted or ruuning, by unique id
        self.alloc_from_task: dict[bytes, Allocation] = {}

        # Tasks currently running on a worker
        self.alloc_from_wuid: dict[SessionUid, Allocation] = {}

        # Tasks queued from clients
        self.pending_requests: dict[ReplyFromSession, gm.Request] = {}

    def on_ready(self, session: ReplyFromSession, msg: gm.Ready
            ) -> list[TransportMessage]:
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
            return []

        # Before sending, mark it as affected to this worker so we can
        # handle errors and free resources later
        self.alloc_from_wuid[session.uid] = pending_alloc

        # Fill in the worker route in original request
        return [session.reply()(pending_alloc.msg)]

    def on_pool_ready(self, pool: ReplyFromSession, msg: gm.PoolReady
            ) -> list[TransportMessage]:
        """
        Register pool route and resources.

        Sends out pending requests if some can now be allocated.
        """
        assert self.write_pool is None

        # We already set the forward-address to None since we will never need to
        # forward anything to pool.
        self.write_pool = pool.reply()
        # Adapt the list of cpus to the requested number of max cpus by dropping
        # or repeting some as needed
        cpu_list = [x for x, _ in zip(cycle(msg.cpus), range(self.max_cpus))]
        self.resources = gtt.Resources(cpus=len(cpu_list), vm='',
                cpu_list=tuple(cpu_list))

        return self.allocate_any()

    def _free_resources(self, session: ReplyFromSession, reuse=True) -> None:
        """
        Add a worker to the idle list after clearing the current task
        information and freeing resources.

        This should not be called directly, use reallocate which ensures the
        free resources are reallocated to pending requests if possible.
        """
        # Get task
        alloc = self.alloc_from_wuid.pop(session.uid, None)
        if alloc:
            # Free resources
            self.resources = free_resources(self.resources, alloc.resources)
            if self.alloc_from_task.pop(alloc.task_id, None) is None:
                logging.error('Double free of allocation %s', alloc)
            # Free worker for reuse if marked as such
            if reuse:
                self.idle_workers[alloc.claim].append(session)
        # Else, the free came from an unmetered source, for instance a PUT sent
        # by a client to a worker

    def allocate_any(self) -> list[TransportMessage]:
        """
        Go through the queue and try to allocate pending requests.
        """
        replies = []
        while self.pending_requests:
            client, request = self.pending_requests.popitem()
            proceeds = self.attempt_allocate(client, request)
            # On first failure, re-queue the failed request, and stop further
            # allocations
            if not proceeds:
                self.pending_requests[client] = request
                break
            replies += proceeds
        return replies

    def reallocate(self, worker: ReplyFromSession, reuse: bool = True
                   ) -> list[TransportMessage]:
        """
        Mark resources as unused, and allocate any possible pending task

        If reuse is false, while the worker will not be directly reallocated, a
        new worker with similar resource requirements may be created, the
        resources may thus be reallocated even if the actual worker isn't.
        """
        self._free_resources(worker, reuse=reuse)
        return self.allocate_any()

    def exited_errors(self, alloc: Allocation, error: str) -> list[TransportMessage]:
        """
        Generate error messages to propagate when a peer exits
        """
        # Note that we set the incoming to empty, which equals re-interpreting
        # the message as addressed to us
        write_client = alloc.client.reply()

        return [add_request_id(write_client, alloc.msg)(gm.RemoteError(error))]

    def on_exited(self, msg: gm.Exited
            ) -> list[TransportMessage]:
        """
        Propagate failure messages and free resources when worker is killed
        """
        peer = msg.peer
        logging.error("Worker %s exited", peer)

        session = self.session_from_peer.get(peer)
        if session is None:
            logging.error("Worker %s is unknown, ignoring exit", peer)
            return []

        alloc = self.alloc_from_wuid.get(session.uid, None)
        if alloc is None:
            logging.error("Worker %s was not assigned a task, ignoring exit", peer)
            return []

        # Allocate new requests based on freed resources, and forward errors to
        # client
        return (
            self.reallocate(session, reuse=False)
            +
            self.exited_errors(alloc, msg.error)
            )

    def calc_resource_claim(self, msg: gm.Message) -> gtt.Resources:
        """
        Determine resources requested by a request
        """
        if isinstance(msg, gm.Submit):
            return msg.task_def.resources
        return gtt.Resources(cpus=1)

    def on_request(self, client: ReplyFromSession, msg: gm.Request
            ) -> list[TransportMessage]:
        """
        Queue or allocate a request
        """
        if client in self.pending_requests:
            logging.error('Dropping %s (queue full)', msg)
            return []

        proceeds = self.attempt_allocate(client, msg)
        if not proceeds:
            self.pending_requests[client] = msg
        return proceeds

    def attempt_allocate(self, client: ReplyFromSession, msg: gm.Request
            ) -> list[TransportMessage]:
        """
        Try to assign a worker.

        If conditions for allocation are not met (waiting for pool or other
        tasks to free resources), return no messages. If conditions are met,
        return messages to be sent to proceed, either to a worker or to the
        pool, plus an event to client to queue next request.
        """
        claim = self.calc_resource_claim(msg)

        # Drop if we already accepted the same task
        # This ideally should not happen if the client receives proper feedback
        # on when to re-submit, but should happen from time to time under normal
        # operation
        task_id = get_request_id(msg).as_word()
        if task_id in self.alloc_from_task:
            logging.error('Reprocessing %s (already allocated)', task_id)

        # Drop if we don't have a way to spawn workers
        if not self.write_pool:
            return []

        # Try allocate and drop if we don't have any resources left
        self.resources, resources = allocate_resources(self.resources, claim)
        if resources is None:
            return []

        # Allocated. At this point the message can be considered
        # as accepted and queued
        proceeds = [client.reply()(gm.NextRequest())]

        # For Submits, the worker will need to know the details of the
        # allocation, so rewrite the original message
        new_msg: gm.Request
        if isinstance(msg, gm.Submit):
            new_msg = gm.Submit(task_def=msg.task_def, resources=resources)
        else:
            new_msg = msg
        del msg

        # Related to issue #87: we should already reply to the client now that
        # we're taking the task
        alloc = Allocation(
                claim=claim,
                resources=resources,
                msg=new_msg,
                client=client,
                task_id=task_id
                )
        self.alloc_from_task[alloc.task_id] = alloc


        # If we have idle workers, directly forward to one
        if (workers := self.idle_workers[claim]):
            worker = workers.pop()
            logging.debug('Worker available, forwarding %s', task_id)

            # Save task info
            self.alloc_from_wuid[worker.uid] = alloc

            # We build the message and return it to transport
            proceeds.append(worker.reply()(new_msg))
        else:
            # Else ask the pool to fork a new worker
            # We'll send the request to the worker when it send us a READY
            proceeds.append(self.write_pool(gm.Fork(alloc.task_id, alloc.claim)))

        return proceeds

    def on_reply(self, session: ReplyFromSession, reply: gm.Reply):
        """
        Forward reply (typically a Progress update) based on request id
        """
        # Get the associated allocation and client
        alloc = self.alloc_from_task.get(reply.request.as_word())

        # Free resources for all messages indicating end of task
        if isinstance(reply.value, gm.Progress):
            replies = []
        else:
            replies = self.reallocate(session)

        # Forward to the waiting client if any
        if alloc:
            replies.append(alloc.client.reply()(reply))

        return replies

    def on_message(self, session: ReplyFromSession, msg: gm.Message
            ) -> list[TransportMessage]:
        """
        Handles local messages received by the broker
        """
        match msg:
            # Record joining peers and forward pre allocated requests
            case gm.Ready():
                return self.on_ready(session, msg)
            case gm.PoolReady():
                return self.on_pool_ready(session, msg)
            # Similarly, record dead peers and forward failures
            case gm.Exited():
                return self.on_exited(msg)
            case gm.Reply():
                return self.on_reply(session, msg)

        # Else, we may have to forward or queue the message. We decide based on
        # the verb whether this should be ultimately sent to a worker
        if isinstance(msg, gm.Request): # type: ignore[arg-type] # mypy bug
            return self.on_request(session, msg) # type: ignore[arg-type] # mypy bug

        # If we reach this point, we received a message we know nothing about
        logging.error('Unknown message %s', msg)
        return []
