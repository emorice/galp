"""
Broker
"""

import argparse
import asyncio
import logging

import zmq

import galp.cli

from galp.cli import IllegalRequestError
from galp.protocol import Protocol
from galp.zmq_async_transport import ZmqAsyncTransport

class Broker:
    def __init__(self, terminate, client_endpoint, worker_endpoint):
        self.terminate = terminate

        # pylint: disable=no-member # False positive
        self.client_proto = BrokerProtocol(self, 'CL')
        self.client_transport = ZmqAsyncTransport(
            self.client_proto,
            client_endpoint, zmq.ROUTER, bind=True)

        self.worker_proto = WorkerProtocol(self, 'WK')
        self.worker_transport = ZmqAsyncTransport(
            self.worker_proto,
            worker_endpoint, zmq.ROUTER, bind=True)

    async def listen_forward_loop(self, source_transport, dest_transport):
        """Client-side message processing loop of the broker"""

        proto_name = source_transport.proto.name
        logging.info("Broker listening for %s on %s", proto_name,
            source_transport.endpoint
            )

        while True:
            try:
                replies = await source_transport.recv_message()
            except IllegalRequestError as err:
                logging.error('Bad %s request: %s', proto_name, err.reason)
                await source_transport.send_message(
                    source_transport.proto.illegal(err.route)
                    )
            await dest_transport.send_messages(replies)

        # FIXME: termination condition ?
        self.terminate.set()

class BrokerProtocol(Protocol):
    """
    Common behavior for both sides
    """
    def on_invalid(self, route, reason):
        raise IllegalRequestError(route, reason)

    def on_unhandled(self, verb):
        """
        For the broker, many messages will just be forwarded and no handler is
        needed.
        """
        logging.debug("No broker action for %s", verb.decode('ascii'))

class WorkerProtocol(BrokerProtocol):
    """
    Handler for messages received from workers.
    """
    def __init__(self, name, router):
        super().__init__(name, router)

        # List of idle workers
        self.idle_workers = []

        # Internal routing id indexed by self-identifiers
        self.route_from_peer = {}
        # Current task from internal routing id
        self.task_from_route = {}


    def on_ready(self, route, peer):
        incoming, forward = route
        assert not forward

        self.route_from_peer[peer] = incoming
        self.idle_workers.append(incoming)

    def mark_worker_available(self, worker_route):
        """
        Add a worker to the idle list after clearing the current task
        information
        """
        # route need to be converted from list to tuple to be used as key
        key = tuple(worker_route)
        if key in self.task_from_route:
            del self.task_from_route[key]
        self.idle_workers.append(worker_route)

    def on_done(self, route, name):
        worker_route, _ = route
        self.mark_worker_available(worker_route)

    async def on_failed(self, route, name):
        await self.mark_worker_available(route)

    async def on_put(self, route, name, proto, data, children):
        await self.mark_worker_available(route)

    async def on_exited(self, route, peer):
        logging.error("Worker %s exited", peer)

        route = self.broker.route_from_peer.get(peer)
        if route is None:
            logging.error("Worker %s is unknown, ignoring exit", peer)
            return

        task = self.broker.task_from_route.get(tuple(route))
        if task is None:
            logging.error("Worker %s was not assigned a task, ignoring exit", peer)
            return

        task_name, client_route = task
        # Normally the other route part is the worker route, but here the
        # worker died so we set it to empty to make sure the client cannot
        # accidentally try to re-use it.
        incoming_route = []

        await self.broker.client.failed((incoming_route, client_route), task_name)

class ForwardProtocol(BrokerProtocol):
    """
    To perform additional actions on messages about to be forwarded to a new worker
    """
    async def on_verb(self, task_route, task_body):
        logging.debug('Task pending')

        worker = await self.broker.workers.get()
        logging.debug('Worker available, forwarding')

        client_route, empty = task_route
        # At this point we should not already know who to forward to
        assert not empty

        # We fill in the forwarding route
        new_route = (client_route, worker)

        # We pass the full route to upper
        await super().on_verb(new_route, task_body)

        await self.broker.worker.send_message_to(new_route, task_body)

    async def on_submit(self, route, name, step_name, vtags, arg_names, kwarg_names):
        # client = incoming, worker = forward
        client_route, worker_route = route
        self.broker.task_from_route[tuple(worker_route)] = (name, client_route)

async def main(args):
    """Entry point for the broker program

    Args:
        args: an object whose attributes are the the parsed arguments, as
            returned by argparse.ArgumentParser.parse_args.
    """
    galp.cli.setup(args, "broker")

    terminate = galp.cli.create_terminate()

    broker = Broker(terminate,
        client_endpoint=args.client_endpoint,
        worker_endpoint=args.worker_endpoint
        )

    tasks = []

    for coro in [
        broker.listen_clients(),
        broker.listen_workers(),
        broker.process_tasks()
        ]:
        tasks.append(asyncio.create_task(coro))

    await galp.cli.wait(tasks)


def add_parser_arguments(parser):
    """Add broker-specific arguments to the given parser"""
    parser.add_argument(
        'client_endpoint',
        help="Endpoint to bind to, in ZMQ format, e.g. tcp://127.0.0.2:12345 "
            "or ipc://path/to/socket ; see also man zmq_bind. The broker will"
            "listen for incoming client connections on this endpoint."
        )
    parser.add_argument(
        'worker_endpoint',
        help="Endpoint to bind to, in ZMQ format, e.g. tcp://127.0.0.2:12345 "
            "or ipc://path/to/socket ; see also man zmq_bind. The broker will"
            "listen for incoming worker connections on this endpoint."
        )

    galp.cli.add_parser_arguments(parser)

if __name__ == '__main__':
    """Convenience hook to start a broker from CLI""" 
    parser = argparse.ArgumentParser()
    add_parser_arguments(parser)
    asyncio.run(main(parser.parse_args()))
