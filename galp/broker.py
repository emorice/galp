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
from galp.zmq_async_protocol import ZmqAsyncProtocol

class Broker:
    def __init__(self, terminate, client_endpoint, worker_endpoint):
        self.terminate = terminate

        self.client = ClientProtocol(self, 'CL', client_endpoint, zmq.ROUTER, bind=True)
        self.worker = WorkerProtocol(self, 'WK', worker_endpoint, zmq.ROUTER, bind=True)

        self.tasks_push = ZmqAsyncProtocol('TQ', 'inproc://tasks', zmq.PUSH, bind=True)
        self.tasks_pull = ForwardProtocol(self, 'FW', 'inproc://tasks', zmq.PULL)

        self.workers = asyncio.Queue()

        # Internal routing id indexed by self-identifiers
        self.route_from_peer = {}
        # Current task from internal routing id
        self.task_from_route = {}

    async def listen_clients(self):
        """Client-side message processing loop of the broker"""

        terminate = False
        logging.info("Broker listening for clients on %s", self.client.endpoint)

        while not terminate:
            # Receive message
            msg = await self.client.socket.recv_multipart()
            # Parse it and handle broker-side actions
            try:
                terminate = await self.client.on_message(msg)
            except IllegalRequestError as err:
                logging.error('Bad client request: %s', err.reason)
                await self.client.send_illegal(err.route)

        self.terminate.set()

    async def listen_workers(self):
        """Worker-side message processing loop of the broker"""

        terminate = False
        logging.info("Broker listening for workers on %s", self.worker.endpoint)

        while not terminate:
            # Reveive message
            msg = await self.worker.socket.recv_multipart()
            # Parse it and handle broker-side actions
            try:
                terminate = await self.worker.on_message(msg)
            except IllegalRequestError as err:
                logging.error('Bad worker request: %s', err.reason)
                await self.worker.send_illegal(err.route)

        self.terminate.set()

    async def process_tasks(self):
        """
        This event loop needs to be cancelled.
        """
        logging.debug('Starting to process task queue')
        while True:
            await self.tasks_pull.process_one()

class BrokerProtocol(ZmqAsyncProtocol):
    """
    Common behavior for both sides
    """
    def __init__(self, broker, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.broker = broker

    def on_invalid(self, route, reason):
        raise IllegalRequestError(route, reason)

    async def on_unhandled(self, verb):
        """
        For the broker, many messages will just be forwarded and no handler is
        needed.
        """
        logging.debug("No broker action for %s", verb.decode('ascii'))

class ClientProtocol(BrokerProtocol):
    """
    Handler for messages received from clients.
    """
    async def on_verb(self, route, msg_body):
        """
        Args:
            route: a tuple (incoming_route, forwarding route)
            msg_body: all the message parts starting with the galp verb
        """
        # Call upper protocol callbacks
        ret = await super().on_verb(route, msg_body)

        # Forward to worker if needed
        incoming_route, forward_route = route
        if forward_route:
            logging.debug('Forwarding %s', msg_body[:1])
            await self.broker.worker.send_message_to(
                route,
                msg_body)
        else:
            logging.debug('Queuing %s', msg_body[:1])
            await self.broker.tasks_push.send_message_to(
                route, msg_body
                )
        return ret

class WorkerProtocol(BrokerProtocol):
    """
    Handler for messages received from workers.
    """
    async def on_verb(self, route, msg_body):
        ret = await super().on_verb(route, msg_body)

        # Forward to client if needed
        incoming_route, forward_route = route
        if forward_route:
            logging.debug('Forwarding %s', msg_body[:1])
            await self.broker.client.send_message_to(
                route, msg_body)
        else:
            logging.debug('Not forwarding %s', msg_body[:1])

        return ret

    async def on_ready(self, route, peer):
        incoming, forward = route
        assert not forward
        self.broker.route_from_peer[peer] = incoming
        await self.broker.workers.put(incoming)

    async def mark_worker_available(self, route):
        worker_route, client_route = route
        key = tuple(worker_route)
        if key in self.broker.task_from_route:
            del self.broker.task_from_route[key]
        await self.broker.workers.put(worker_route)

    async def on_done(self, route, name):
        await self.mark_worker_available(route)

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
    async def on_unhandled(self, verb):
        return super().on_unhandled(verb)

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
