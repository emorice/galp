"""
Lists of internal commands
"""

import sys
import time
from enum import Enum
from collections import deque

import logging

from .graph import task_deps, TaskReference, TaskType

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
        new_sub_commands = []
        # Aggregate status of all sub-commands
        sub_status = self.script.status_conj(sub_commands)
        while sub_status != Status.PENDING:
            # By default, we stop processing a command on failures. In theory
            # the command could have other independent work to do, but we have
            # no use case yet.
            # Note: this drops new_sub_commands by design since there is no need
            # to inject them into the event loop with the parent failed.
            if sub_status == Status.FAILED:
                self.result = next(
                        sub.result
                        for sub in sub_commands
                        if sub.status == Status.FAILED)
                return Status.FAILED, []

            # At this point, all sub commands are DONE

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

            new_sub_commands = sub_commands

            logging.debug('%s: %s => %s', self, self._state, new_state)
            self._state = new_state
            if self._state in (Status.DONE, Status.FAILED):
                assert not new_sub_commands
                return self._state, []

            # Re-inspect status of new set of sub-commands and loop
            sub_status = self.script.status_conj(sub_commands)

        # Ensure all remaining sub-commands have downstream links pointing to
        # this command before relinquishing control
        for sub in sub_commands:
            sub.out(self)

        # Save for callback
        self._sub_commands = sub_commands

        return Status.PENDING, new_sub_commands

    def advance(self, external_status=None):
        """
        Try to advance the commands state. If the move causes new commands to be
        created or moves to a terminal state and trigger downstream commands,
        returns them for them to be advanced by the caller too.
        """
        new_sub_commands = []

        # If command is already in terminal state, do nothing. In theory, the
        # rest of the procedure should be a no-op anyway, but that makes it
        # easier e.g. to log things only once.
        if self.status != Status.PENDING:
            # Avoid silently ignoring conflicting external status
            assert external_status is None or external_status == self.status
            return []
        old_status = self.status

        # Command is pending, i.e. waiting for an external trigger. Maybe this
        # trigger came since the last advance attempt, so we try to move states.
        if external_status is None:
            # TODO: this should also return possible child commands.
            self.status, new_sub_commands = self._eval()
        else:
            self.status = external_status

        # Give the script the chance to hook anything, like printing out progress
        if self.script:
            self.script.notify_change(self, old_status, self.status)

        # Also log the change anyway
        logging.info('%s %s', self.status.name.ljust(7), self)

        # Maybe this caused the command to advance to a terminal state (DONE or
        # FAILED). In that case downstream commands must be checked too.
        if self.status != Status.PENDING:
            return new_sub_commands + list(self.outputs)

        # Else, still pending, nothing else to check
        return new_sub_commands

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
        """
        Externally change the status of the command, then try to advance it
        """
        self.result = result
        advance_all(self.advance(status))

    def do_once(self, verb, name, *args, **kwargs):
        """
        Creates a unique command
        """
        key =  verb, name
        cls = _once_commands[verb]

        if key in self.script.commands:
            cmd = self.script.commands[key]
            return cmd
        cmd = cls(self.script, name, *args, **kwargs)
        self.script.commands[key] = cmd
        self.script.new_commands.append(key)
        return cmd

    @property
    def _str_res(self):
        return (
            f' = [{", ".join(str(r) for r in self.result)}]'
            if self.is_done() else ''
            )

def advance_all(commands):
    """
    Try to advance all given commands, and all downstream depending on them the
    case being
    """
    commands = list(commands)
    while commands:
        command = commands.pop()
        commands.extend(command.advance())

class UniqueCommand(Command):
    """
    Any command that is fully defined and identified by a task name
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
        keep_going: whether to attempt to complete independent commands when one
            fails. If False, attempts to "fail fast" instead, and finishes as
            soon as any command fails.
    """
    def __init__(self, verbose=True, store=None, keep_going=False):
        super().__init__(self)
        self.commands = {}
        self.new_commands = deque()
        self.verbose = verbose
        self.keep_going=keep_going
        self.store = store

        self._task_dicts = {}

    def collect(self, commands):
        """
        Creates a command representing a collection of other commands
        """
        return Collect(self, commands)

    def callback(self, command, callback):
        """
        Adds an arbitrary callback to an existing command. This triggers
        execution of the command as far as possible, and of the callback if
        ready.
        """
        cb_command = Callback(command, callback)
        # The callback is set as a downstream link to the command, but it still
        # need an external trigger, so we need to advance it
        advance_all([command])
        return cb_command

    def notify_change(self, command, old_status, new_status):
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
                task_done, task_dict, _children = command.result
                if not task_done and 'step_name' in task_dict:
                    self._task_dicts[command.name] = task_dict
                    print_status('PLAN', command.name,
                            task_dict['step_name'].decode('ascii'))

        if isinstance(command, Submit):
            if command.name in self._task_dicts:
                step_name_s = self._task_dicts[command.name]['step_name'].decode('ascii')

                # We log 'SUB' every time a submit command's status "changes" to
                # PENDING. Currently this only happens when the command is
                # created.
                if new_status == Status.PENDING:
                    print_status('SUB', command.name, step_name_s)
                elif new_status == Status.DONE:
                    print_status('DONE', command.name, step_name_s)
                if new_status == Status.FAILED:
                    print_status('FAIL', command.name, step_name_s)

    def advance(self, *_, **_k):
        return []

    def __str__(self):
        return 'script'

    def status_conj(self, commands):
        """
        Status conjunction respecting self.keep_going
        """
        status = [cmd.status for cmd in commands]

        if all(s == Status.DONE for s in status):
            return Status.DONE

        if self.keep_going:
            if any(s == Status.PENDING for s in status):
                return Status.PENDING
            return Status.FAILED

        if any(s == Status.FAILED for s in status):
            return Status.FAILED
        return Status.PENDING

    def run_task(self, task, dry=False):
        """
        Creates command appropriate to type of task
        """
        if hasattr(task, 'query'):
            if dry:
                raise NotImplementedError('Cannot dry-run queries yet')
            return Query(self.script, task.subject.name, task.query)

        if dry:
            return self.do_once('DRYRUN', task.name)
        return self.do_once('RUN', task.name)

class InertCommand(UniqueCommand):
    """
    Command tied to an external event
    """
    def __str__(self):
        return f'{self.__class__.__name__.lower().ljust(7)} {self.name}' + self._str_res

    def _eval(self):
        return Status.PENDING, []

@once
class Get(InertCommand):
    """
    Get a single resource part
    """

@once
class Submit(InertCommand):
    """
    Remotely execute a single step
    """

@once
class Stat(InertCommand):
    """
    Get a task's metadata
    """

@once
class Rget(UniqueCommand):
    """
    Recursively gets a resource and all its parts
    """
    when = When()

    @when('INIT')
    def _init(self):
        return 'PARENT', self.do_once('GET', self.name)

    @when('PARENT')
    def _get(self, get):
        return 'CHILDREN', [
                self.do_once('RGET', child_name)
                for child_name in get.result
                ]

    @when('CHILDREN')
    def _sub_gets(self, *_):
        return Status.DONE

class Collect(Command):
    """
    A collection of other commands
    """
    def __init__(self, script, commands):
        self.commands = commands
        super().__init__(script)

    def __str__(self):
        return 'collect'

    when = When()

    @when('INIT')
    def _init(self):
        return 'COLLECT', self.commands

    @when('COLLECT')
    def _end(self, *commands):
        self.result = [ c.result for c in commands ]
        return Status.DONE

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
            return Status.DONE, []
        return Status.PENDING, []

@once
class SSubmit(UniqueCommand):
    """
    A non-recursive ("simple") task submission: executes dependencies, but not
    children. Return said children as result on success.
    """
    _dry = False
    when = When()

    @when('INIT')
    def _init(self):
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
        _task_done, task_dict, children = stat.result

        # Short circuit for tasks already processed and literals
        if 'children' in task_dict:
            children = task_dict['children']

        if children is not None:
            self.result = children
            return Status.DONE

        # Query, should never have reached this layer as queries have custom run
        # mechanics
        if 'query' in task_dict:
            raise NotImplementedError

        # Else, regular job, process dependencies first
        cmd = 'DRYRUN' if self._dry else 'RSUBMIT'
        return 'DEPS', [
                self.do_once(cmd, dep)
                for dep in task_deps(task_dict)
                ]

    @when('DEPS')
    def _deps(self, *_):
        """
        Check deps availability before proceeding to main submit
        """
        # For dry-runs, bypass the submission
        if self._dry:
            # Explicitely set the result to "no children"
            self.result = []
            return Status.DONE

        # Task itself
        return 'MAIN', self.do_once('SUBMIT', self.name)

    @when('MAIN')
    def _main(self, main_sub):
        """
        Check the main result and return possible children tasks
        """
        self.result = main_sub.result
        return Status.DONE

@once
class DrySSubmit(SSubmit):
    """
    Dry-run variant of SSubmit
    """
    _dry = True

@once
class RSubmit(UniqueCommand):
    """
    Recursive submit, with children, aka an SSubmit plus a RSubmit per child
    """
    _dry = False

    when = When()

    @when('INIT')
    def _init(self):
        """
        Emit the simple submit
        """
        cmd = 'DRYSSUBMIT' if self._dry else 'SSUBMIT'
        return 'MAIN', self.do_once(cmd, self.name)

    @when('MAIN')
    def _main(self, ssub):
        """
        Check the main result and proceed with possible children tasks
        """
        children = ssub.result

        cmd = 'DRYRUN' if self._dry else 'RSUBMIT'
        return 'CHILDREN', [ self.do_once(cmd, child_name)
                for child_name in children ]

    @when('CHILDREN')
    def _children(self, *_):
        """
        Check completion of children and propagate result
        """
        return Status.DONE

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
    def _init(self):
        """
        Process the query and decide on further commands to issue
        """

        # Simple queries
        # ==============

        # Query terminator, this is a query leaf and requests the task result
        # itself ; issue a RUN
        if self.query is True:
            return 'RUN', self.do_once('RUN', self.subject)

        # Status of task, issue a STAT command
        if self.query in ('$done', ('$done', True)):
            return 'STATUS', self.do_once('STAT', self.subject)

        # Definition of task, issue a STAT command
        if self.query in ('$def', ('$def', True)):
            return 'DEF', self.do_once('STAT', self.subject)

        # Base of task, issue a SRUN
        if self.query in ('$base', ('$base', True)):
            return 'BASE', self.do_once('SRUN', self.subject)

        # Recursive queries
        # ================

        # Query set, turn each in its own query
        if hasattr(self.query, 'items'):
            return 'QUERYSET', [
                    Query(self.script, self.subject, (key, value))
                    for key, value in self.query.items()
                    ]

        # Query items
        try:
            query_key, _sub_query = self.query
        except (TypeError, ValueError):
            raise NotImplementedError(self.query) from None
        if not isinstance(query_key, str):
            raise NotImplementedError(self.query)

        # Arg query, get the task definition first
        if query_key == '$args':
            return 'ARGS', self.do_once('STAT', self.subject)

        # Children query, run the task first
        if query_key == '$children':
            return 'CHILDREN', self.do_once('SSUBMIT', self.subject)

        # Iterator, get the raw object first
        if query_key == '*':
            return 'ITERATOR', self.do_once('SRUN', self.subject)

        # Direct indexing, get raw object first
        if query_key.isidentifier():
            return 'INDEX', self.do_once('SRUN', self.subject)


        raise NotImplementedError(self.query)

    @when('STATUS')
    def _status(self, stat_cmd):
        """
        Check and returns status request
        """
        task_done, *_ = stat_cmd.result

        self.result = task_done
        return Status.DONE

    @when('DEF')
    def _def(self, stat_cmd):
        """
        Check and returns definition request
        """
        _done, task_dict, _children = stat_cmd.result

        self.result = task_dict
        return Status.DONE

    @when('ARGS')
    def _args(self, stat_cmd):
        """
        Build list of sub-queries for arguments of subject task from the
        definition obtained from STAT
        """
        _task_done, task_dict, _children = stat_cmd.result
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
                Query(self.script, target, subquery)
                )
        return 'DICT_QUERYSET', sub_commands

    @when('CHILDREN')
    def _children(self, ssub_cmd):
        """
        Build a list of sub-queries for children of subject task
        from the children list obtained from SRUN
        """
        children = ssub_cmd.result

        _, child_subqueries = self.query
        sub_commands = []

        if '*' in child_subqueries:
            if len(child_subqueries) > 1:
                raise NotImplementedError('Only one subquery supported when '
                        'using universal children queries at the moment')
            child_subqueries = { i: child_subqueries['*']
                    for i in range(len(children))
                    }

        for index, subquery in child_subqueries.items():
            try:
                num_index = int(index)
                target = children[num_index]
            except ValueError: # key-indexed child
                raise NotImplementedError(index) from None
            sub_commands.append(
                Query(self.script, target, subquery)
                )
        return 'DICT_QUERYSET', sub_commands

    @when('DICT_QUERYSET')
    def _dict_queryset(self, *query_items):
        """
        Check and merge argument query items
        """
        _, arg_subqueries = self.query
        if '*' in arg_subqueries:
            arg_subqueries = range(len(query_items))

        result = {}
        for index, qitem in zip(arg_subqueries, query_items):
            result[str(index)] = qitem.result
        self.result = result

        return Status.DONE

    @when('QUERYSET')
    def _queryset(self, *query_items):
        """
        Check and merge query items
        """
        result = {}
        for qitem in query_items:
            result[qitem.query[0]] = qitem.result
        self.result = result
        return Status.DONE

    @when('ITERATOR')
    def _iterator(self, _):
        """
        Check simple run, extract raw result and build subqueries
        """
        shallow_obj = self.script.store.get_native(self.subject, shallow=True)

        _, subquery = self.query
        subquery_commands = []

        for task in shallow_obj:
            if not isinstance(task, TaskType):
                raise TypeError('Object is not a collection of tasks, cannot'
                    ' use a "*" query here')
            subquery_commands.append(
                    Query(self.script, task.name, subquery)
                    )

        return 'ITEMS', subquery_commands

    @when('ITEMS')
    def _items(self, *items):
        """
        Check and pack items
        """
        self.result = [ item.result for item in items ]
        return Status.DONE

    @when('INDEX')
    def _index(self, _):
        """
        Subquery for a simple item
        """
        shallow_obj = self.script.store.get_native(self.subject, shallow=True)

        index, subquery = self.query

        try:
            task = shallow_obj[index]
        except Exception as exc:
            raise RuntimeError(
                f'Query failed, cannot access item "{index}" of task {self.subject}'
                ) from exc

        if not isinstance(task, TaskType):
            raise TypeError(
                f'Item "{index}" of task {self.subject} is not itself a '
                f'task, cannot apply subquery "{subquery}" to it'
                )

        return 'ITEM', Query(self.script, task.name, subquery)

    @when('ITEM')
    def _item(self, item):
        """
        Return result of simple indexed command
        """
        self.result = item.result
        return Status.DONE


    @when('BASE')
    def _base(self, _):
        """
        Return base result
        """
        self.result = self.script.store.get_native(self.subject, shallow=True)
        return Status.DONE

    @when('RUN')
    def _run(self, _):
        """
        Return full result
        """
        self.result = self.script.store.get_native(self.subject, shallow=False)
        return Status.DONE

# Despite the name, since a DryRun never does any fetches, it is implemented as
# a special case of RSubmit and not Run (Run is RSubmit + RGet)
@once
class DryRun(RSubmit):
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
    _dry = True

@once
class Run(UniqueCommand):
    """
    Combined rsubmit + rget
    """
    when = When()

    @when('INIT')
    def _init(self):
        return 'RSUB', self.do_once('RSUBMIT', self.name)

    @when('RSUB')
    def _rsub(self, _):
        return 'RGET', self.do_once('RGET', self.name)

    @when('RGET')
    def _rsub(self, _):
        return Status.DONE

@once
class SRun(UniqueCommand):
    """
    Shallow run: combined ssubmit + get, fetches the raw result of a task but
    not its children
    """
    when = When()

    @when('INIT')
    def _init(self):
        return 'SSUB', self.do_once('SSUBMIT', self.name)

    @when('SSUB')
    def _ssub(self, _):
        return 'GET', self.do_once('GET', self.name)

    @when('GET')
    def _rget(self, _):
        return Status.DONE
