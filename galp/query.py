"""
Implementation of complex queries within the command asynchronous system
"""

import logging
from typing import Type

import galp.net.requests.types as gr
from . import commands as cm
from . import task_types as gtt
from .task_types import TaskNode, QueryTaskDef, CoreTaskDef
from .cache import StoreReadError

def run_task(task: TaskNode, dry: bool = False) -> cm.Command:
    """
    Creates command appropriate to type of task (query or non-query)
    """
    task_def = task.task_def
    if isinstance(task_def, QueryTaskDef):
        if dry:
            raise NotImplementedError('Cannot dry-run queries yet')
        subject, = task.dependencies
        # Issue #81 unsafe reference
        return query(subject, task_def.query)

    return cm.run(task, dry)

def query(subject: gtt.Task, _query):
    """
    Process the query and decide on further commands to issue
    """
    operator = query_to_op(subject, _query)

    return operator.requires(subject).then(
            lambda *required: cm.Gather(operator.recurse(*required)).then(
                operator.safe_result
                )
            )

def parse_query(_query):
    """
    Split query into query key and sub-query, watching out for shorthands

    Returns:
        Pair (is_compound: bool, object) where object is a mapping if compound,
        else itself a pair (query_key, sub_query).
    """
    # Scalar shorthand: query terminator, implicit sub
    if _query is True:
        return False, ('$sub', [])

    # Scalar shorthand: just a string, implicit terminator
    if isinstance(_query, str):
        return False, (_query, [])

    # Directly a mapping
    if hasattr(_query, 'items'):
        return True, _query

    # Else, either regular string-sub_query sequence, or singleton mapping
    # expected
    try:
        query_item, *sub_query = _query
    except (TypeError, ValueError):
        raise NotImplementedError(_query) from None

    if hasattr(query_item, 'items'):
        if sub_query:
            # Compound _query followed by a subquery, nonsense
            raise NotImplementedError(_query)
        return True, query_item

    if not isinstance(query_item, str):
        raise NotImplementedError(_query)

    # Normalize the _query terminator
    if len(sub_query) == 1 and sub_query[0] is True:
        sub_query = []

    return False, (query_item, sub_query)

class Operator:
    """
    Register an operator requiring the given command
    """
    def __init__(self, subject: gtt.Task, sub_query):
        self.sub_query = sub_query
        self.subject = subject
        # Result of the command specified in requires
        self._required = None

    @staticmethod
    def requires(task: gtt.Task) -> cm.Command:
        """
        Pre-req
        """
        _ = task
        return cm.Gather([])

    _ops: 'dict[str, Type[Operator]]' = {}
    def __init_subclass__(cls, /, named=True, **kwargs):
        super().__init_subclass__(**kwargs)
        if named:
            name = cls.__name__.lower()
            cls._ops[name] = cls

    @classmethod
    def by_name(cls, name) -> 'type[Operator] | None':
        """
        Return operator by dollar-less name or None
        """
        return cls._ops.get(name)

    def recurse(self, required=None):
        """
        Build subqueries if needed
        """
        self._required = required
        return self._recurse(required)

    def _recurse(self, _required):
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
        return self._result(self._required, subs)

    def _result(self, _required, _sub_cmds):
        return None

    def safe_result(self, subs):
        """
        Wrapper around result for common exceptions
        """
        try:
            return self.result(subs)
        except StoreReadError  as exc:
            logging.exception('In %s:', self)
            return cm.Failed(exc)

def query_to_op(subject: gtt.Task, query_doc) -> Operator:
    """
    Convert query into operator
    """
    is_compound, parsed_query = parse_query(query_doc)
    # Query set, turn each in its own query
    if is_compound:
        return Compound(subject, parsed_query.items())

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
        return op_cls(subject, sub_query)

    ## Direct index
    if query_key.isidentifier() or query_key.isnumeric():
        index = int(query_key) if query_key.isnumeric() else query_key
        return GetItem(subject, sub_query, index)

    ## Iterator
    if query_key == '*':
        return Iterate(subject, sub_query)

    raise NotImplementedError(query_doc)

class Base(Operator):
    """
    Base operator, returns shallow object itself with the linked tasks left as
    references
    """
    @staticmethod
    def requires(task: gtt.Task):
        return cm.sget(task.name)

    def _result(self, sget_result, _subs):
        return sget_result

class Sub(Operator):
    """
    Sub operator, returns subtree object itself with the linked tasks resolved
    """
    requires = staticmethod(lambda task: cm.rget(task.name))

    def _result(self, rget_result, _subs):
        return rget_result

class Done(Operator):
    """
    Completion state operator
    """
    @staticmethod
    def requires(task: gtt.Task):
        return cm.Stat(task.name)

    def _result(self, stat_result: gr.Done | gr.Found, _subs):
        return isinstance(stat_result, gr.Done)

class Def(Operator):
    """
    Definition oerator
    """
    @staticmethod
    def requires(task: gtt.Task):
        return cm.safe_stat(task)

    def _result(self, stat_result: gr.Done | gr.Found, _subs):
        return stat_result.task_def

class Args(Operator):
    """
    Task arguments operator
    """
    @staticmethod
    def requires(task: gtt.Task):
        return cm.safe_stat(task)

    def _recurse(self, stat_result: gr.Done | gr.Found):
        """
        Build list of sub-queries for arguments of subject task from the
        definition obtained from STAT
        """
        task_def = stat_result.task_def

        # Issue 80: this should delegate handling of the sub-query
        is_compound, arg_subqueries = parse_query(self.sub_query)
        if not is_compound:
            raise NotImplementedError('Args operator requires mapping as sub-query')

        # bare list of indexes, set the sub_query to the whole object
        if not hasattr(arg_subqueries, 'items'):
            arg_subqueries = { index: True for index in arg_subqueries }

        if not isinstance(task_def, CoreTaskDef):
            raise TypeError('Object is not a job-type Task, cannot use a "args"'
                + ' query here.')

        sub_commands = []
        for index, sub_query in arg_subqueries.items():
            try:
                num_index = int(index)
                target = task_def.args[num_index].name
            except ValueError: # keyword argument
                target = task_def.kwargs[index].name
            sub_commands.append(
                query(gtt.TaskRef(target), sub_query)
                )
        return sub_commands

    def _result(self, _stat_cmd, query_items):
        """
        Merge argument query items
        """
        return merge_query_items(self.sub_query, query_items)

def merge_query_items(sub_query, query_items):
    """
    Builds a dictionary of results from the a flattened list of subquerys
    """
    # Issue 80: this should delegate handling of the sub-query
    _is_compound, arg_subqueries = parse_query(sub_query)
    if '*' in arg_subqueries:
        arg_subqueries = range(len(query_items))

    result = {}
    for index, qitem in zip(arg_subqueries, query_items):
        result[str(index)] = qitem

    return result

class Children(Operator):
    """
    Recurse on children after simple task run
    """
    requires = staticmethod(cm.ssubmit)

    def _recurse(self, result: gtt.ResultRef):
        """
        Build a list of sub-queries for children of subject task
        from the children list obtained from SRUN
        """
        child_subqueries = self.sub_query
        sub_commands = []

        # Issue 80: this should delegate handling of the sub-query
        is_compound, child_subqueries = parse_query(child_subqueries)
        if not is_compound:
            raise NotImplementedError('Children operator requires mapping as sub-query')

        if '*' in child_subqueries:
            if len(child_subqueries) > 1:
                raise NotImplementedError('Only one sub_query supported when '
                        + 'using universal children queries at the moment')
            child_subqueries = { i: child_subqueries['*']
                    for i in range(len(result.children))
                    }
        for index, sub_query in child_subqueries.items():
            try:
                num_index = int(index)
                target = result.children[num_index]
            except ValueError: # key-indexed child
                raise NotImplementedError(index) from None
            sub_commands.append(
                query(target, sub_query)
                )
        return sub_commands

    def _result(self, _stat_cmd, query_items):
        """
        Merge argument query items
        """
        return merge_query_items(self.sub_query, query_items)

class GetItem(Operator, named=False):
    """
    Item access parametric operator
    """
    def __init__(self, subject, sub_query, index):
        super().__init__(subject, sub_query)
        self.index = index

    requires = staticmethod(cm.srun)

    def _recurse(self, srun_result):
        """
        Subquery for a simple item
        """
        shallow_obj = srun_result
        sub_query = self.sub_query

        try:
            task = shallow_obj[self.index]
        except Exception as exc:
            raise RuntimeError(
                f'Query failed, cannot access item "{self.index}" of task '
                f'{self.subject}'
                ) from exc

        if not isinstance(task, gtt.Task):
            raise TypeError(
                f'Item "{self.index}" of task {self.subject} is not itself a '
                f'task, cannot apply sub_query "{sub_query}" to it'
                )

        return query(gtt.TaskRef(task.name), sub_query)

    def _result(self, _srun_cmd, subs):
        """
        Return result of simple indexed command
        """
        item, = subs
        return item

class Compound(Operator, named=False):
    """
    Collection of several other operators
    """
    def __init__(self, subject, sub_queries):
        super().__init__(subject, None)
        self.sub_queries = sub_queries

    def _recurse(self, _no_cmd):
        return [
            query(self.subject, sub_query)
            for sub_query in self.sub_queries
            ]

    def _result(self, _no_cmd, results):
        return { query[0]: result
                for query, result in zip(self.sub_queries, results)}

class Iterate(Operator, named=False):
    """
    Iterate over object
    """
    requires = staticmethod(cm.srun)

    def _recurse(self, srun_result):
        """
        Extract raw result and build subqueries
        """
        shallow_obj = srun_result

        sub_query = self.sub_query
        sub_query_commands = []

        # Try to extract the subtask definitions from the node object if
        # possible
        if isinstance(self.subject, gtt.TaskNode):
            task_nodes = {t.name: t for t in self.subject.dependencies}
        else:
            task_nodes = {}

        for task in shallow_obj:
            if not isinstance(task, gtt.Task):
                raise TypeError('Object is not a collection of tasks, cannot'
                    + ' use a "*" query here')
            # Issue # 81
            sub_query_commands.append(
                    query(task_nodes.get(task.name, gtt.TaskRef(task.name)),
                        sub_query)
                    )

        return sub_query_commands

    def _result(self, _srun_cmd, results):
        """
        Pack items
        """
        return list(results)
