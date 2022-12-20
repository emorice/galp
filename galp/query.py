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

        is_compound, parsed_query = parse_query(self.query)
        # Query set, turn each in its own query
        if is_compound:
            self.op = Compound(self, parsed_query.items())
        else:
            # Simple queries, parse into key-sub_query
            query_key, sub_query = parsed_query

            # Find operator by key
            ## Regular named operator
            if query_key.startswith('$'):
                op_cls = Operator.by_name(query_key[1:])
                if op_cls is None:
                    raise NotImplementedError(
                        f'No such operator: "{query_key}"'
                        )
                self.op = op_cls(self, sub_query)

            ## Direct index
            if query_key.isidentifier() or query_key.isnumeric():
                index = int(query_key) if query_key.isnumeric() else query_key
                self.op = GetItem(self, sub_query, index)

            ## Iterator
            if query_key == '*':
                self.op = Iterate(self, sub_query)

        if self.op is None:
            raise NotImplementedError(self.query)

        if self.op.requires is None:
            return '_operator'

        return '_operator', self.do_once(self.op.requires, self.subject)

    def _operator(self, *command):
        """
        Execute handler for operator after the required commands are done
        """
        return '_operator_result', self.op.recurse(*command)

    def _operator_result(self, *sub_commands):
        """
        Execute handler for operator after the required commands are done
        """
        self.result = self.op.result(sub_commands)
        return Status.DONE

def parse_query(query):
    """
    Split query into query key and sub-query, watching out for shorthands

    Returns:
        Pair (is_compound: bool, object) where object is a mapping if compound,
        else itself a pair (query_key, sub_query).
    """
    # Scalar shorthand: query terminator, implicit sub
    if query is True:
        return False, ('$sub', [])

    # Scalar shorthand: just a string, implicit terminator
    if isinstance(query, str):
        return False, (query, [])

    # Directly a mapping
    if hasattr(query, 'items'):
        return True, query

    # Else, either regular string-sub_query sequence, or singleton mapping
    # expected
    try:
        query_item, *sub_query = query
    except (TypeError, ValueError):
        raise NotImplementedError(query) from None

    if hasattr(query_item, 'items'):
        if sub_query:
            # Compound query followed by a subquery, nonsense
            raise NotImplementedError(query)
        return True, query_item

    if not isinstance(query_item, str):
        raise NotImplementedError(query)

    # Normalize the query terminator
    if len(sub_query) == 1 and sub_query[0] is True:
        sub_query = []

    return False, (query_item, sub_query)

class Operator:
    """
    Register an operator requiring the given command
    """
    def __init__(self, query, sub_query):
        self.sub_query = sub_query
        self.script = query.script
        self.store = query.script.store
        self.subject = query.subject
        self._req_cmd = None

    requires = None

    _ops = {}
    def __init_subclass__(cls, /, named=True, **kwargs):
        super().__init_subclass__(**kwargs)
        if named:
            name = cls.__name__.lower()
            cls._ops[name] = cls

    @classmethod
    def by_name(cls, name):
        """
        Return operator by dollar-less name or None
        """
        return cls._ops.get(name)

    def recurse(self, cmd=None):
        """
        Build subqueries if needed
        """
        self._req_cmd = cmd
        return self._recurse(cmd)

    def _recurse(self, _cmd):
        """
        Default implementation does not recurse, but checks that no sub-query
        was provided
        """
        if len(self.sub_query) > 0:
            raise NotImplementedError('Operator '
            f'"{self.__class__.__name__.lower()}" does not accept sub-queries '
            f'(sub-query was "{self.sub_query}")')
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
        return self.store.get_native(
                self.subject, shallow=True)

class Sub(Operator):
    """
    Sub operator, returns subtree object itself with the linked tasks resolved
    """
    requires = 'RUN'

    def _result(self, _run_cmd, _subs):
        return self.store.get_native(
                self.subject, shallow=False)

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

        # FIXME: this should delegate handling of the sub-query
        is_compound, arg_subqueries = parse_query(self.sub_query)
        if not is_compound:
            raise NotImplementedError('Args operator requires mapping as sub-query')

        # bare list of indexes, set the sub_query to the whole object
        if not hasattr(arg_subqueries, 'items'):
            arg_subqueries = { index: True for index in arg_subqueries }

        if not 'arg_names' in task_dict:
            raise TypeError('Object is not a job-type Task, cannot use a "args"'
                ' query here.')

        sub_commands = []
        for index, sub_query in arg_subqueries.items():
            try:
                num_index = int(index)
                target = task_dict['arg_names'][num_index]
            except ValueError: # keyword argument
                target = task_dict['kwarg_names'][index]
            sub_commands.append(
                Query(self.script, target, sub_query)
                )
        return sub_commands

    def _result(self, _stat_cmd, query_items):
        """
        Merge argument query items
        """
        # FIXME: this should delegate handling of the sub-query
        is_compound, arg_subqueries = parse_query(self.sub_query)
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

        child_subqueries = self.sub_query
        sub_commands = []

        # FIXME: this should delegate handling of the sub-query
        is_compound, child_subqueries = parse_query(child_subqueries)
        if not is_compound:
            raise NotImplementedError('Children operator requires mapping as sub-query')

        if '*' in child_subqueries:
            if len(child_subqueries) > 1:
                raise NotImplementedError('Only one sub_query supported when '
                        'using universal children queries at the moment')
            child_subqueries = { i: child_subqueries['*']
                    for i in range(len(children))
                    }
        for index, sub_query in child_subqueries.items():
            try:
                num_index = int(index)
                target = children[num_index]
            except ValueError: # key-indexed child
                raise NotImplementedError(index) from None
            sub_commands.append(
                Query(self.script, target, sub_query)
                )
        return sub_commands

class GetItem(Operator, named=False):
    """
    Item access parametric operator
    """
    def __init__(self, query, sub_query, index):
        super().__init__(query, sub_query)
        self.index = index

    requires = 'SRUN'

    def _recurse(self, _srun_cmd):
        """
        Subquery for a simple item
        """
        shallow_obj = self.store.get_native(self.subject, shallow=True)
        sub_query = self.sub_query

        try:
            task = shallow_obj[self.index]
        except Exception as exc:
            raise RuntimeError(
                f'Query failed, cannot access item "{self.index}" of task '
                f'{self.subject}'
                ) from exc

        if not isinstance(task, TaskType):
            raise TypeError(
                f'Item "{self.index}" of task {self.subject} is not itself a '
                f'task, cannot apply sub_query "{sub_query}" to it'
                )

        return Query(self.script, task.name, sub_query)

    def _result(self, _srun_cmd, subs):
        """
        Return result of simple indexed command
        """
        item, = subs
        return item.result

class Compound(Operator, named=False):
    """
    Collection of several other operators
    """
    def __init__(self, query, sub_queries):
        super().__init__(query, None)
        self.sub_queries = sub_queries

    def _recurse(self, _no_cmd):
        return [
            Query(self.script, self.subject, sub_query)
            for sub_query in self.sub_queries
            ]

    def _result(self, _no_cmd, sub_cmds):
        return { sub_cmd.query[0]: sub_cmd.result
                for sub_cmd in sub_cmds }

class Iterate(Operator, named=False):
    """
    Iterate over object
    """
    requires = 'SRUN'

    def _recurse(self, _srun_cmd):
        """
        Extract raw result and build subqueries
        """
        shallow_obj = self.store.get_native(self.subject, shallow=True)

        sub_query = self.sub_query
        sub_query_commands = []

        for task in shallow_obj:
            if not isinstance(task, TaskType):
                raise TypeError('Object is not a collection of tasks, cannot'
                    ' use a "*" query here')
            sub_query_commands.append(
                    Query(self.script, task.name, sub_query)
                    )

        return sub_query_commands

    def _result(self, _srun_cmd, items):
        """
        Pack items
        """
        return [ item.result for item in items ]
