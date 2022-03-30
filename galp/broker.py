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

        self.forward = ForwardProtocol(self, 'FW')

        self.workers = asyncio.Queue()

        ctx = zmq.asyncio.Context()
        self.tasks_push = ctx.socket(zmq.PUSH)
        self.tasks_pull = ctx.socket(zmq.PULL)
        self.tasks_push.bind('inproc://tasks')
        self.tasks_pull.connect('inproc://tasks')

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
            # Forward to worker if needed
            msg_body, route = self.worker.get_routing_parts(msg)
            incoming_route, forward_route = self.forward.split_route(route)
            if forward_route:
                logging.debug('Forwarding %s', msg_body[:1])
                await self.worker.send_message_to(
                    forward_route + incoming_route,
                    msg_body)
            else:
                logging.debug('Queuing %s', msg_body[:1])
                await self.tasks_push.send_multipart(
                    incoming_route + [b''] + msg_body
                    )

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
            # Forward to client if needed
            msg_body, route = self.worker.get_routing_parts(msg)
            incoming_route, forward_route = self.worker.split_route(route)
            if forward_route:
                logging.debug('Forwarding %s', msg_body[:1])
                await self.client.send_message_to(
                    forward_route + incoming_route,
                    msg_body)
            else:
                logging.debug('Not forwarding %s', msg_body[:1])

        self.terminate.set()

    async def process_tasks(self):
        """
        This event loop needs to be cancelled.
        """
        logging.debug('Starting to process task queue')
        while True:
            task = await self.tasks_pull.recv_multipart()
            task_body, task_route = self.client.get_routing_parts(task)
            logging.debug('Task pending')
            worker = await self.workers.get()
            logging.debug('Worker available, forwarding')

            new_msg = self.forward.build_message(
                worker + task_route, task_body
                )

            await self.forward.on_message(new_msg)

            await self.worker.send_message(new_msg)


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

class WorkerProtocol(BrokerProtocol):
    """
    Handler for messages received from clients.
    """
    async def on_ready(self, route, peer):
        self.broker.route_from_peer[peer] = route
        await self.broker.workers.put(route[:1])


    async def mark_worker_available(self, route):
        worker_route, client_route = self.split_route(route)
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
        await self.broker.client.failed(client_route, task_name)

class ForwardProtocol(Protocol):
    """
    To perform additional actions on messages about to be forwarded to a new worker
    """
    def __init__(self, broker, name):
        super().__init__(name)
        self.broker = broker

    async def on_unhandled(self, verb):
        return super().on_unhandled(verb)

    async def on_submit(self, route, name, step_name, vtags, arg_names, kwarg_names):
        worker_route, client_route = self.split_route(route)
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
