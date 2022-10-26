"""
Lists of internal commands
"""

import sys
import time
from enum import Enum
from collections import deque

import logging

from .graph import task_deps, TaskReference, TaskType

def status_conj(commands):
    """
    Ternary conjunction of an iterable of status
    """
    status = [cmd.status for cmd in commands]
    if all(s == Status.DONE for s in status):
        return Status.DONE
    if any(s == Status.FAILED for s in status):
        return Status.FAILED
    return Status.PENDING

def status_conj_any(commands):
    """
    Ternary conjunction of an iterable of status
    """
    status = [cmd.status for cmd in commands]
    if all(s != Status.PENDING for s in status):
        return Status.DONE
    return Status.PENDING

class Status(Enum):
    """
    Command status
    """
    PENDING = 'P'
    DONE = 'D'
    FAILED = 'F'

    def __bool__(self):
        return self != Status.DONE

_once_commands = {}
def once(cls):
    """
    Register a class for use by `do_once`
    """
    _once_commands[cls.__name__.upper()] = cls
    return cls

class Command:
    """
    An asynchronous command
    """
    def __init__(self, script):
        self.script = script
        self.status = Status.PENDING
        self.result = None
        self.outputs = set()
        self._state = 'INIT'
        self._sub_commands = []
        self.update(init=True)

    when = {}

    def out(self, cmd):
        """
        Add command to the set of outputs, returns self for easy chaining.
        """
        self.outputs.add(cmd)
        return self

    def _eval(self):
        """
        State-based handling
        """
        sub_commands = self._sub_commands
        while all(cmd.status != Status.PENDING for cmd in sub_commands):
            try:
                handler = self.when[self._state]
            except KeyError:
                raise NotImplementedError(self.__class__, self._state) from None

            ret = handler(self, *sub_commands)
            # Sugar: allow omitting sub commands or passing only one instead of
            # a list
            if isinstance(ret, tuple):
                new_state, sub_commands = ret
            else:
                new_state, sub_commands = ret, []
            if isinstance(sub_commands, Command):
                sub_commands = [sub_commands]

            self._sub_commands = sub_commands
            # Returning pending from a handlers means 'stay in the same state'
            if new_state == Status.PENDING:
                new_state = self._state
            logging.debug('%s: %s => %s', self, self._state, new_state)
            self._state = new_state
            if self._state in (Status.DONE, Status.FAILED):
                return self._state

        return Status.PENDING

    def change_status(self, new_status, init=False):
        """
        Updates status, triggering callbacks and collecting replies
        """
        old_status = self.status
        self.status = new_status
        if self.script:
            self.script.notify_change(self, old_status, new_status, init)

        logging.info('%s %s',
                ('NEW' if init else new_status.name).ljust(7),
                self
                )
        if old_status == Status.PENDING and new_status != Status.PENDING:
            for command in self.outputs:
                command.update()

    def update(self, init=False):
        """
        Re-evals condition and possibly trigger callbacks, returns a list of
        replies
        """
        if self.status == Status.PENDING:
            self.change_status(self._eval(), init)

    def done(self, result=None):
        """
        Mark the future as done and runs callbacks.

        A task can be safely marked several times, the callbacks are run only
        when the status transitions from unknown to known.

        However successive done calls will overwrite the result.
        """
        return self._mark_as(Status.DONE, result)

    def is_done(self):
        """
        Boolean, if command done
        """
        return self.status == Status.DONE

    def is_failed(self):
        """
        Boolean, if command done
        """
        return self.status == Status.FAILED

    def is_pending(self):
        """
        Boolean, if command still pending
        """
        return self.status == Status.PENDING

    def failed(self, result=None):
        """
        Mark the future as failed and runs callbacks.

        A task can be safely marked several times, the callbacks are run only
        when the status transitions from unknown to known.

        However successive done calls will overwrite the result.
        """
        return self._mark_as(Status.FAILED, result)

    def _mark_as(self, status, result):
        self.result = result
        self.change_status(status)

    def do_once(self, verb, name, *args, **kwargs):
        """
        Creates a unique command
        """
        key =  verb, name
        cls = _once_commands[verb]

        if key in self.script.commands:
            cmd = self.script.commands[key]
            cmd.out(self)
            return cmd
        cmd = cls(self.script, name, *args, **kwargs)
        cmd.out(self)
        self.script.commands[key] = cmd
        self.script.new_commands.append(key)
        return cmd

    def req(self, *commands):
        """
        Aggregate failure status of commands, propagating result of first failure
        """

        for command in commands:
            if command.is_failed():
                self.result = command.result
                break

        return status_conj(commands)

    @property
    def _str_res(self):
        return (
            f' = [{", ".join(str(r) for r in self.result)}]'
            if self.is_done() else ''
            )

class UniqueCommand(Command):
    """
    Any command that is fully defined and indentified by a task name
    """
    def __init__(self, script, name):
        self.name = name
        super().__init__(script)

    def __str__(self):
        return f'{self.__class__.__name__.lower()} {self.name}'

class When:
    """
    Decorator factory to make declaring automata simpler
    """
    def __init__(self):
        self._functions = {}

    def __call__(self, key):
        """
        Decorator to register a method as handler for a particular state (Use
        the instance of the When class as the decorator)
        """
        def _wrapper(function):
            self._functions[key] = function
            return function
        return _wrapper

    def __getitem__(self, key):
        """
        Accessor for the registered decorated methods
        """
        return self._functions[key]

class Script(Command):
    """
    A collection of maker methods for Commands

    Args:
        verbose: whether to print brief summary of command completion to stderr
        store: Store object, destination to store Query result. Only needed for
            queries.
    """
    def __init__(self, verbose=True, store=None):
        super().__init__(self)
        self.commands = {}
        self.new_commands = deque()
        self.verbose = verbose
        self.store = store

        self._task_dicts = {}

    def collect(self, commands, allow_failures=False):
        """
        Creates a command representing a collection of other commands
        """
        return Collect(self, commands, allow_failures)

    def callback(self, command, callback):
        """
        Adds an arbitrary callback to an existing command
        """
        return Callback(command, callback)

    def notify_change(self, command, old_status, new_status, init):
        """
        Hook called when the graph status changes
        """
        if not self.verbose:
            return
        del old_status

        def print_status(stat, name, step_name):
            print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} {stat:4} [{name}]'
                    f' {step_name}',
                    file=sys.stderr, flush=True)

        if isinstance(command, Stat):
            if new_status == Status.DONE:
                task_done, task_dict, *_ = command.result
                if not task_done and 'step_name' in task_dict:
                    self._task_dicts[command.name] = task_dict
                    print_status('PLAN', command.name,
                            task_dict['step_name'].decode('ascii'))

        if isinstance(command, Submit):
            if command.name in self._task_dicts:
                step_name_s = self._task_dicts[command.name]['step_name'].decode('ascii')

                if init:
                    print_status('SUB', command.name, step_name_s)
                elif new_status == Status.DONE:
                    print_status('DONE', command.name, step_name_s)
                if new_status == Status.FAILED:
                    print_status('FAIL', command.name, step_name_s)

    def update(self, *_, **_k):
        pass

    def __str__(self):
        return 'script'

@once
class Get(Command):
    """
    Get a single resource part
    """
    def __init__(self, script, name):
        self.name = name
        super().__init__(script)

    def __str__(self):
        return f' get {self.name}' + self._str_res

    def _eval(self):
        return Status.PENDING

@once
class Rget(UniqueCommand):
    """
    Recursively gets a resource and all its parts
    """
    when = When()

    @when('INIT')
    def _init(self, *_):
        return 'PARENT', self.do_once('GET', self.name)

    @when('PARENT')
    def _get(self, get):
        failed = self.req(get)
        if failed:
            return failed

        return 'CHILDREN', [
                self.do_once('RGET', child_name)
                for child_name in get.result
                ]

    @when('CHILDREN')
    def _sub_gets(self, *sub_gets):
        return self.req(*sub_gets), sub_gets

class Collect(Command):
    """
    A collection of other commands
    """
    def __init__(self, script, commands, allow_failures):
        self.commands = commands
        self.allow_failures = allow_failures
        for cmd in commands:
            cmd.out(self)
        super().__init__(script)

    def __str__(self):
        return 'collect'

    def _eval(self):
        if self.allow_failures:
            return status_conj_any(self.commands)

        return self.req(*self.commands)

class Callback(Command):
    """
    An arbitrary callback function, used to tie in other code
    """
    def __init__(self, command, callback):
        self._callback = callback
        self._in = command
        self._in.out(self)
        super().__init__(script=None)

    def __str__(self):
        return f'callback {self._callback.__name__}'

    def _eval(self):
        if self._in.status != Status.PENDING:
            self._callback(self._in.status)
            return Status.DONE
        return Status.PENDING

@once
class SSubmit(UniqueCommand):
    """
    A non-recursive ("simple") task submission: executes depndencies, but not
    children. Return said children as result on success.
    """
    when = When()

    @when('INIT')
    def _init(self, *_):
        """
        Emit the initial stat
        """
        # metadata
        return 'SORT', self.do_once('STAT', self.name)

    @when('SORT')
    def _sort(self, stat):
        """
        Sort out queries, remote jobs and literals
        """
        sta = self.req(stat)
        if sta:
            return sta

        task_done, *stat_result = stat.result

        # Short circuit for tasks already processed and literals
        children, task_dict = None, None
        if task_done:
            task_dict, children = stat_result
        else:
            task_dict, = stat_result
            children = task_dict.get('children')

        if children is not None:
            self.result = children
            return Status.DONE

        # Queries, wrap in the dedicated command
        if 'query' in task_dict:
            query_cmd = Query(self.script, task_dict['subject'], task_dict['query'])
            query_cmd.out(self)
            return 'QUERY', query_cmd

        # Else, regular job, process dependencies first
        return 'DEPS', [
                self.do_once('RSUBMIT', dep)
                for dep in task_deps(task_dict)
                ]

    @when('DEPS')
    def _deps(self, *deps):
        """
        Check deps availability before proceeding to main submit
        """
        sta = self.req(*deps)
        if sta:
            return sta

        # task itself
        return 'MAIN', self.do_once('SUBMIT', self.name)

    @when('MAIN')
    def _main(self, main_sub):
        """
        Check the main result and return possible children tasks
        """
        sta = self.req(main_sub)
        if sta:
            return sta

        self.result = main_sub.result
        return Status.DONE


    @when('QUERY')
    def _query(self, query_cmd):
        """
        Check query completion and put result in store
        """
        not_done = self.req(query_cmd)
        if not_done:
            return not_done

        # Check result in store
        store = self.script.store
        child_names = store.put_native(self.name, query_cmd.result)

        # The final result of a query may reference other tasks, so these need
        # to be executed as children
        self.result = child_names
        return Status.DONE

@once
class RSubmit(UniqueCommand):
    """
    Recursive submit, with children, aka an SSubmit plus a RSubmit per child
    """
    when = When()

    @when('INIT')
    def _init(self, *_):
        """
        Emit the simple submit
        """
        ssub = self.do_once('SSUBMIT', self.name)
        return 'MAIN', ssub

    @when('MAIN')
    def _main(self, ssub):
        """
        Check the main result and proceed with possible children tasks
        """
        sta = self.req(ssub)
        if sta:
            return sta

        children = ssub.result

        return 'CHILDREN', [ self.do_once('RSUBMIT', child_name)
                for child_name in children ]

    @when('CHILDREN')
    def _children(self, *children):
        """
        Check completion of children and propagate result
        """
        return self.req(*children)

class Query(Command):
    """
    Local execution of a query

    Args:
        subject: the name of the task to apply the query to
        query: the native object representing the query
    """
    def __init__(self, script, subject, query):
        self.subject = subject
        self.query = query
        super().__init__(script)

    def __str__(self):
        return f'query on {self.subject} {self.query}'

    when = When()

    @when('INIT')
    def _init(self, *_):
        """
        Process the query and decide on further commands to issue
        """

        # Simple queries
        # ==============

        # Query terminator, this is a query leaf and requests the task result
        # itself
        if self.query is True:
            # Return a reference to the task
            self.result = TaskReference(self.subject)
            return Status.DONE

        # Status of task, issue a STAT command
        if self.query in ('done', ('done', True)):
            return 'STATUS', self.do_once('STAT', self.subject)

        # Recursive queries
        # ================

        # Query set, turn each in its own query
        if hasattr(self.query, 'items'):
            return 'QUERYSET', [
                    Query(self.script, self.subject, (key, value)).out(self)
                    for key, value in self.query.items()
                    ]

        # Query items
        try:
            query_key, _sub_query = self.query
        except TypeError:
            raise NotImplementedError(self.query) from None

        # Iterator, get the raw object first
        if query_key == '*':
            return 'ITERATOR', self.do_once('SRUN', self.subject)

        # Arg query, get the task definition first
        if query_key == 'args':
            return 'ARGS', self.do_once('STAT', self.subject)

        raise NotImplementedError(self.query)

    @when('STATUS')
    def _status(self, stat_cmd):
        """
        Check and returns status request
        """
        not_done = self.req(stat_cmd)
        if not_done:
            return not_done

        task_done, _ = stat_cmd.result

        self.result = task_done
        return Status.DONE

    @when('ARGS')
    def _args(self, stat_cmd):
        """
        Build list of sub-queries for arguments of subject task from the
        definition obtained from STAT
        """
        not_done = self.req(stat_cmd)
        if not_done:
            return not_done

        _task_done, task_dict, *_children = stat_cmd.result
        _, arg_subqueries = self.query

        # bare list of indexes, set the subquery to the whole object
        if not hasattr(arg_subqueries, 'items'):
            arg_subqueries = { index: True for index in arg_subqueries }

        if not 'arg_names' in task_dict:
            raise TypeError('Object is not a job-type Task, cannot use a "args"'
                ' query here.')

        sub_commands = []
        for index, subquery in arg_subqueries.items():
            try:
                num_index = int(index)
                target = task_dict['arg_names'][num_index]
            except ValueError: # keyword argument
                target = task_dict['kwarg_names'][index]
            sub_commands.append(
                Query(self.script, target, subquery).out(self)
                )
        return 'ARG_QUERYSET', sub_commands

    @when('ARG_QUERYSET')
    def _queryset(self, *query_items):
        """
        Check and merge argument query items
        """
        not_done = self.req(*query_items)
        if not_done:
            return not_done

        _, arg_subqueries = self.query

        result = {}
        for index, qitem in zip(arg_subqueries, query_items):
            result[index] = qitem.result
        self.result = result

        return Status.DONE

    @when('QUERYSET')
    def _queryset(self, *query_items):
        """
        Check and merge query items
        """
        not_done = self.req(*query_items)
        if not_done:
            return not_done

        result = {}
        for qitem in query_items:
            result[qitem.query[0]] = qitem.result
        self.result = result
        return Status.DONE

    @when('ITERATOR')
    def _iterator(self, srun):
        """
        Check simple run, extract raw result and build subqueries
        """
        sta = self.req(srun)
        if sta:
            return sta

        shallow_obj = self.script.store.get_native(self.subject, shallow=True)

        _, subquery = self.query
        subquery_commands = []

        for task in shallow_obj:
            if not isinstance(task, TaskType):
                raise TypeError('Object is not a collection of tasks, cannot'
                    ' use a "*" query here')
            subquery_commands.append(
                    Query(self.script, task.name, subquery).out(self)
                    )

        return 'ITEMS', subquery_commands

    @when('ITEMS')
    def _items(self, *items):
        """
        Check and pack items
        """
        not_done = self.req(*items)
        if not_done:
            return not_done

        self.result = [ item.result for item in items ]
        return Status.DONE

@once
class DryRun(UniqueCommand):
    """
    A task dry-run. Similar to RSUBMIT, but does not actually submit tasks, and
    instead moves on as soon as the STAT command succeeded.

    If a task has dynamic children (i.e., running the task returns new tasks to
    execute), the output will depend on the task status:
     * If the task is done, the dry-run will be able to list the child tasks and
       recurse on them.
     * If the task is not done, the dry-run will not see the child tasks and
       skip them, so only the parent task will show up in the dry-run log.

    This is the intended behavior, it reflects the inherent limit of a dry-run:
    part of the task graph is unknown before we actually start executing it.
    """
    def _eval(self):
        stat = self.do_once('STAT', self.name)
        sta = self.req(stat)
        if sta:
            return sta

        task_done, stat_result = stat.result

        if not task_done:
            task_dict = stat_result

            # dependencies
            sta = self.req(*[
                self.do_once('DRYRUN', dep)
                for dep in task_deps(task_dict)
                ])
            if sta:
                return sta

            # Skip running the task itself, assume no children
            children = []
        else:
            task_dict, children = stat_result

        # Recursive children tasks
        return self.req(*[
            self.do_once('DRYRUN', child_name)
            for child_name in children
            ])

@once
class Run(UniqueCommand):
    """
    Combined rsubmit + rget
    """
    def _eval(self):
        rsub = self.do_once('RSUBMIT', self.name)

        sta = self.req(rsub)
        if sta:
            return sta

        rget = self.do_once('RGET', self.name)

        return self.req(rget)

@once
class SRun(UniqueCommand):
    """
    Shallow run: combined ssubmit + get, fetches the raw result of a task but
    not its children
    """
    def _eval(self):
        rsub = self.do_once('SSUBMIT', self.name)

        sta = self.req(rsub)
        if sta:
            return sta

        rget = self.do_once('GET', self.name)

        return self.req(rget)

@once
class Submit(Command):
    """
    Get a single resource part
    """
    def __init__(self, script, name):
        self.name = name
        super().__init__(script)

    def __str__(self):
        return f' submit {self.name}' + self._str_res

    def _eval(self):
        return Status.PENDING

@once
class Stat(Command):
    """
    Get a task's metadata
    """
    def __init__(self, script, name):
        self.name = name
        super().__init__(script)

    def __str__(self):
        return f' stat {self.name}' + self._str_res

    def _eval(self):
        return Status.PENDING
