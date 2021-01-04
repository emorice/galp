"""
Client api.

In contrast with a worker which is written as a standalone process, a client is just an
object that can be created and used as part of a larger program.
"""

import logging
import json

from collections import defaultdict

class Client:
    """
    A client that communicate with a worker

    Args:
        socket: a ZeroMQ asyncio socket, already bound to the worker endpoint.
            The client will not attempt to close it. No read operation must be
            attempted by external code while the client exists. New write,
            connect and binds should not be a problem.
    """

    # For our sanity, below this point, by 'task' here we need task _name_, except for the
    # 'tasks' public argument itself. The tasks themselves are called 'details'

    def __init__(self, socket):
        self.socket = socket

        # Public attributes: counters for the number of SUBMITs sent and DOING
        # received for each task
        # Used for reporting and testing cache behavior
        self.submitted_count = defaultdict(int)
        self.run_count = defaultdict(int)

        self._details = dict()
        self._dependents = defaultdict(set)
        self._dependencies = defaultdict(set)
        self._done = defaultdict(bool) # def to False

        # Ordered !
        self._finals = list()
        self._resources = dict()

    def add(self, tasks):
        """
        Browse the graph and add it to the tasks we're tracking, in an
        idempotent way. Also tracks which one are final.

        The client can keep references to any task passed to it directly or as
        a dependency. Modifying tasks after there were added, directly or as
        dependencies to any degree, results in undefined behaviour. Creating new
        tasks depending on already added ones, however, is safe. In other words,
        you can add new downstream steps, but not change the upstream part.

        Return the names of all new tasks without dependencies. 

        This justs updates the client's state, so it can never block and is a
        sync function.
        """

        # ! Ordered !
        for task_details in tasks:
            self._finals.append(task_details.name)

        # Build the inverse dependencies lookup table, i.e the 'children' task.
        # child/parent can be ambiguous here (what is the direction of the
        # arcs ?) so we call them "dependents" as opposed to "dependencies"

        # Todo: Optimisation: sets and defaultdicts make most operations safely
        # idempotent, but we could skip tasks added in a previous call
        oset, cset = set(tasks), set()

        new_top_level = set()
        while oset:
            # Get a task
            task_details = oset.pop()
            task = task_details.name
            cset.add(task)

            # Add the links
            for dep_details in task_details.dependencies:
                dep = dep_details.name
                self._dependencies[task].add(dep)
                self._dependents[dep].add(task)
                if dep not in cset:
                    oset.add(dep_details)

            # Check unseen input tasks
            if not self._dependencies[task] and task not in self._details:
                new_top_level.add(task)

            # Save details, and mark as seen
            self._details[task] = task_details

        return new_top_level

    async def submit_ready_dependents(self, task_name):
        """Given that task_name just finished, check if any dependent is ready
        to be submitted, and if so do it.

        As it calls submit, it can block if submission can not be made.
        """
        done_task = task_name

        self._done[done_task] = True
        for dept in self._dependents[done_task]:
            if all(self._done[sister_dep] for sister_dep in self._dependencies[dept]):
                await self.submit(dept)

    async def collect(self, *tasks):
        """
        Recursively submit the tasks, wait for completion, fetches, deserialize
        and returns the actual results.

        Written as a coroutine, so that it can be called inside an asynchronous
        application, such as a worker or proxy or whatever: the caller is in
        charge of the event loop.
        """

        # Update our state
        new_inputs = self.add(tasks)

        for task in new_inputs:
            await self.submit(task)

        # Todo: for now we don't care if an other collect is running, so if
        # you start two you will miss the final events.
        # Todo: timeouts
        logging.warning('Now reacting to completion events')
        while True:
            msg = await self.socket.recv_multipart()
            logging.warning('Received: %s', msg[0])
            assert msg[0] != b'ILLEGAL'
            if msg[0] == b'DONE':
                await self.on_done(msg)
            elif msg[0] == b'DOING':
                logging.warning('Doing: %s', msg[1].hex())
                self.run_count[msg[1]] += 1
            elif msg[0] == b'PUT':
                await self.on_put(msg)
                if all(task in self._resources for task in self._finals):
                    break
            else:
                logging.warn('Unexpected', msg[0].hex())

        return [self._resources[task] for task in self._finals]

    async def on_done(self, msg):
        """

        Sends new requests, so can block
        """
        # todo: validate !
        done_task = msg[1]

        await self.submit_ready_dependents(done_task)

        if done_task in self._finals:
            await self.get(done_task)

    async def on_put(self, msg):
        """
        Cannot actually block but async anyway for consistency
        """
        # todo: validate !
        self._resources[msg[1]] = json.loads(msg[2])

    async def submit(self, task):
        """Submit task with given name"""
        details = self._details[task]
        msg = [b'SUBMIT', details.step.key]
        for arg in details.args:
            msg += [ b'', arg.name ]
        for kw, kwarg in details.kwargs.items():
            msg += [ kw.encode('ascii'), kwarg.name ]
        await self.socket.send_multipart(msg)

        self.submitted_count[task] += 1

    async def get(self, task):
        """Send get for task with given name"""
        msg = [b'GET', task]
        await self.socket.send_multipart(msg)

