"""
Implementation of complex queries within the command asynchronous system
"""

from .graph import TaskType
from .commands import Command, Status

def run_task(script, task, dry=False):
    """
    Creates command appropriate to type of task (query or non-query)
    """
    if hasattr(task, 'query'):
        if dry:
            raise NotImplementedError('Cannot dry-run queries yet')
        return Query(script, task.subject.name, task.query)

    if dry:
        return script.do_once('DRYRUN', task.name)
    return script.do_once('RUN', task.name)

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
        self.op = None
        super().__init__(script)

    def __str__(self):
        return f'query on {self.subject} {self.query}'

    def _init(self):
        """
        Process the query and decide on further commands to issue
        """

        # Query terminator, this is a query leaf and requests the task result
        # itself ; issue a RUN
        if self.query is True:
            return '_run', self.do_once('RUN', self.subject)

        # Query set, turn each in its own query
        if hasattr(self.query, 'items'):
            return '_queryset', [
                    Query(self.script, self.subject, (key, value))
                    for key, value in self.query.items()
                    ]

        # Regular query, string optionally followed by subquery
        if isinstance(self.query, str):
            query_key, subquery = self.query, True
        else:
            try:
                query_key, subquery = self.query
            except (TypeError, ValueError):
                raise NotImplementedError(self.query) from None
            if not isinstance(query_key, str):
                raise NotImplementedError(self.query)

        # Operators
        if query_key.startswith('$'):
            op_cls = Operator.by_name(query_key[1:])
            if op_cls is None:
                raise NotImplementedError(
                    f'No such operator: "{query_key}"'
                    )
            self.op = op_cls(self)
            if self.op.requires is None:
                return '_operator'
            return '_operator', self.do_once(self.op.requires, self.subject)

        # Query items

        # Iterator, get the raw object first
        if query_key == '*':
            return '_iterator', self.do_once('SRUN', self.subject)

        # Direct indexing, get raw object first
        if query_key.isidentifier() or query_key.isnumeric():
            return '_index', self.do_once('SRUN', self.subject)

        raise NotImplementedError(self.query)

    def _queryset(self, *query_items):
        """
        Check and merge query items
        """
        result = {}
        for qitem in query_items:
            result[qitem.query[0]] = qitem.result
        self.result = result
        return Status.DONE

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

        return '_items', subquery_commands

    def _items(self, *items):
        """
        Check and pack items
        """
        self.result = [ item.result for item in items ]
        return Status.DONE

    def _index(self, _):
        """
        Subquery for a simple item
        """
        shallow_obj = self.script.store.get_native(self.subject, shallow=True)

        index, subquery = self.query

        try:
            task = shallow_obj[int(index) if index.isnumeric() else index]
        except Exception as exc:
            raise RuntimeError(
                f'Query failed, cannot access item "{index}" of task {self.subject}'
                ) from exc

        if not isinstance(task, TaskType):
            raise TypeError(
                f'Item "{index}" of task {self.subject} is not itself a '
                f'task, cannot apply subquery "{subquery}" to it'
                )

        return '_item', Query(self.script, task.name, subquery)

    def _item(self, item):
        """
        Return result of simple indexed command
        """
        self.result = item.result
        return Status.DONE

    def _run(self, _):
        """
        Return full result
        """
        self.result = self.script.store.get_native(self.subject, shallow=False)
        return Status.DONE

    def _operator(self, command):
        """
        Execute handler for operator after the required commands are done
        """
        return '_operator_result', self.op.recurse(command)

    def _operator_result(self, *sub_commands):
        """
        Execute handler for operator after the required commands are done
        """
        self.result = self.op.result(sub_commands)
        return Status.DONE

class Operator:
    """
    Register an operator requiring the given command
    """
    def __init__(self, query):
        self.query = query
        self._req_cmd = None

    requires = None

    _ops = {}
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        name = cls.__name__.lower()
        cls._ops[name] = cls

    @classmethod
    def by_name(cls, name):
        """
        Return operator by dollar-less name or None
        """
        return cls._ops.get(name)

    def recurse(self, cmd):
        """
        Build subqueries if needed
        """
        self._req_cmd = cmd
        return self._recurse(cmd)

    def _recurse(self, _cmd):
        return []

    def result(self, subs):
        """
        Build result of operator from subcommands
        """
        return self._result(self._req_cmd, subs)

    def _result(self, _req_cmd, _sub_cmds):
        return None

class Base(Operator):
    """
    Base operator, returns shallow object itself with the linked tasks left as
    references
    """
    requires = 'SRUN'

    def _result(self, _srun_cmd, _subs):
        return self.query.script.store.get_native(
                self.query.subject, shallow=True)

class Done(Operator):
    """
    Completion state operator
    """
    requires = 'STAT'

    def _result(self, stat_cmd, _subs):
        task_done, *_ = stat_cmd.result
        return task_done

class Def(Operator):
    """
    Definition oerator
    """
    requires = 'STAT'

    def _result(self, stat_cmd, _subs):
        _done, task_dict, _children = stat_cmd.result
        return task_dict

class Args(Operator):
    """
    Task arguments operator
    """
    requires = 'STAT'

    def _recurse(self, stat_cmd):
        """
        Build list of sub-queries for arguments of subject task from the
        definition obtained from STAT
        """
        _task_done, task_dict, _children = stat_cmd.result
        _, arg_subqueries = self.query.query

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
                Query(self.query.script, target, subquery)
                )
        return sub_commands

    def _result(self, _stat_cmd, query_items):
        """
        Merge argument query items
        """
        _, arg_subqueries = self.query.query
        if '*' in arg_subqueries:
            arg_subqueries = range(len(query_items))

        result = {}
        for index, qitem in zip(arg_subqueries, query_items):
            result[str(index)] = qitem.result

        return result

class Children(Args):
    """
    Recurse on children after simple task run
    """
    requires = 'SSUBMIT'

    def _recurse(self, ssub_cmd):
        """
        Build a list of sub-queries for children of subject task
        from the children list obtained from SRUN
        """
        children = ssub_cmd.result

        _, child_subqueries = self.query.query
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
                Query(self.query.script, target, subquery)
                )
        return sub_commands
