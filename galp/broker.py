"""
Broker
"""

import argparse
import asyncio
import logging

import zmq

import galp.cli

from galp.protocol import IllegalRequestError
from galp.forward_protocol import ForwardProtocol
from galp.zmq_async_transport import ZmqAsyncTransport

class Broker:
    def __init__(self, terminate, client_endpoint, worker_endpoint):
        self.terminate = terminate

        # pylint: disable=no-member # False positive
        self.client_proto = BrokerProtocol('CL', router=True)
        self.client_transport = ZmqAsyncTransport(
            self.client_proto,
            client_endpoint, zmq.ROUTER, bind=True)

        self.worker_proto = WorkerProtocol('WK', router=True)
        self.worker_transport = ZmqAsyncTransport(
            self.worker_proto,
            worker_endpoint, zmq.ROUTER, bind=True)

    async def listen_forward_loop(self, source_transport, dest_transport):
        """Client-side message processing loop of the broker"""

        proto_name = source_transport.protocol.proto_name
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

    async def listen_clients(self):
        """Listen client-side, forward to workers"""
        return await self.listen_forward_loop(
            self.client_transport,
            self.worker_transport)

    async def listen_workers(self):
        """Listen worker-side, forward to clients"""
        return await self.listen_forward_loop(
            self.worker_transport,
            self.client_transport)

class BrokerProtocol(ForwardProtocol):
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

    def on_failed(self, route, name):
        worker_route, _ = route
        self.mark_worker_available(worker_route)

    def on_put(self, route, name, serialized):
        worker_route, _ = route
        self.mark_worker_available(worker_route)

    def on_exited(self, route, peer):
        logging.error("Worker %s exited", peer)

        route = self.route_from_peer.get(peer)
        if route is None:
            logging.error("Worker %s is unknown, ignoring exit", peer)
            return None

        task = self.task_from_route.get(tuple(route))
        if task is None:
            logging.error("Worker %s was not assigned a task, ignoring exit", peer)
            return None

        command, client_route = task
        command_name, task_name = command
        # Normally the other route part is the worker route, but here the
        # worker died so we set it to empty to make sure the client cannot
        # accidentally try to re-use it.
        new_route = [], client_route

        if command_name == b'GET':
            return self.not_found(new_route, task_name)
        if command_name == b'SUBMIT':
            return self.failed(new_route, task_name)
        logging.error(
            'Worker %s died while handling %s, no error propagation',
            peer, command
            )
        return None


    def write_message(self, msg):
        route, msg_body = msg
        incoming_route, forward_route = route
        verb = msg_body[0].decode('ascii')

        # If a forward route is already present, the message is addressed at one
        # specific worker, forward as-is
        if forward_route:
            logging.debug('Forwarding %s', verb)
            return super().write_message(msg)

        # Else, it is addressed to any worker, we pick one if available
        if self.idle_workers:
            worker_route = self.idle_workers.pop()
            logging.debug('Worker available, forwarding %s', verb)

            # We save the up to two components of the message body for forensics
            # if the worker dies, along with the client route
            self.task_from_route[tuple(worker_route)] = (msg_body[:2], incoming_route)

            # We fill in the forwarding route
            new_route = (incoming_route, worker_route)

            # We build the message and return it to transport
            return super().write_message((new_route, msg_body))

        # Finally, with no route and no worker available, we drop the message
        logging.info('Dropping %s', verb)
        return None

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
    # Convenience hook to start a broker from CLI
    _parser = argparse.ArgumentParser()
    add_parser_arguments(_parser)
    asyncio.run(main(_parser.parse_args()))
