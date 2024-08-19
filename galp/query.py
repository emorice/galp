"""
Implementation of complex queries within the command asynchronous system
"""

import logging
from typing import Type, Iterable

from galp.result import Ok, Error
import galp.net.core.types as gm
from galp.store import StoreReadError
from . import commands as cm
from . import task_types as gtt
from .task_types import TaskNode, QueryDef, CoreTaskDef
from .asyn import as_command, collect, collect_dict

def run_task(task: gtt.Task, options: cm.ExecOptions
             ) -> cm.CommandLike[object]:
    """
    Creates command appropriate to type of task (query or non-query)
    """
    if isinstance(task, TaskNode):
        task_def = task.task_def
        if isinstance(task_def, QueryDef):
            if options.dry:
                raise NotImplementedError('Cannot dry-run queries yet')
            subject, = task.inputs
            # Issue #81 unsafe reference
            return as_command(query(subject, task_def.query))

    return cm.run(task, options)

def query(subject: gtt.Task, _query) -> cm.CommandLike:
    """
    Process the query and decide on further commands to issue
    """
    return query_to_op(subject, _query)()

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
    def requires(task: gtt.Task) -> cm.CommandLike:
        """
        Pre-req
        """
        _ = task
        return cm.collect([], keep_going=False)

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

    def __call__(self) -> cm.CommandLike:
        """
        Old-style operators
        """
        return self.requires(self.subject).then(
                lambda *required: cm.collect(
                    self.recurse(*required), keep_going=False
                    ).then(self.safe_result)
                )

    def recurse(self, required=None) -> list[cm.Command]:
        """
        Build subqueries if needed
        """
        self._required = required
        return self._recurse(required)

    def _recurse(self, _required) -> list[cm.Command]:
        """
        Default implementation does not recurse, but checks that no sub-query
        was provided
        """
        if len(self.sub_query) > 0:
            raise NotImplementedError('Operator '
            f'"{self.__class__.__name__.lower()}" does not accept sub-queries '
            f'(sub-query was "{self.sub_query}")')
        return []

    def result(self, subs) -> cm.CommandLike:
        """
        Build result of operator from subcommands
        """
        return self.inner_result(self._required, subs)

    def inner_result(self, _required, _sub_cmds) -> cm.CommandLike[object]:
        """
        Build result of operator from subcommands
        """
        return Error(NotImplemented)

    def safe_result(self, subs) -> cm.CommandLike:
        """
        Wrapper around result for common exceptions
        """
        try:
            return self.result(subs)
        except StoreReadError  as exc:
            logging.exception('In %s:', self)
            return Error(exc)

def _task_input_to_query(task: gtt.Task, tin: gtt.TaskInput
                         ) -> cm.CommandLike[object]:
    return query(get_task_dependency(task, tin.name), tin.op)

def collect_task_inputs(task: gtt.Task, task_def: gtt.CoreTaskDef
                    ) -> cm.Command[tuple[list, dict]]:
    """
    Gather all arguments of a core task
    """
    return collect([
        collect([
            _task_input_to_query(task, tin)
            for tin in task_def.args
            ], keep_going=False),
        collect_dict({
            k: _task_input_to_query(task, tin)
            for k, tin in task_def.kwargs.items()
            }, keep_going=False)
        ], keep_going=False).then(
                # cannot express that collect preserve the inner types
                lambda both: Ok(tuple(both)) # type: ignore[arg-type]
                )

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
    def __call__(self) -> cm.CommandLike:
        return cm.sget(self.subject)

class Sub(Operator):
    """
    Sub operator, returns subtree object itself with the linked tasks resolved
    """
    def __call__(self) -> cm.CommandLike[object]:
        return cm.rget(self.subject)

class Done(Operator):
    """
    Completion state operator
    """
    def __call__(self) -> cm.Command:
        return cm.Send(gm.Stat(self.subject.name)).then(
                lambda statr: Ok(statr.result is not None)
                )

class Def(Operator):
    """
    Definition oerator
    """
    def __call__(self) -> cm.Command:
        return cm.safe_stat(self.subject).then(
                lambda statr: Ok(statr.task_def)
                )

class Args(Operator):
    """
    Task arguments operator
    """
    @staticmethod
    def requires(task: gtt.Task):
        return cm.safe_stat(task)

    def _recurse(self, stat_result: cm.Found):
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
                target_name = task_def.args[num_index].name
            except ValueError: # keyword argument
                target_name = task_def.kwargs[index].name
            target = get_task_dependency(self.subject, target_name)
            sub_commands.append(
                query(target, sub_query)
                )
        return sub_commands

    def inner_result(self, _stat_cmd, query_items) -> Ok[dict]:
        """
        Merge argument query items
        """
        return Ok(merge_query_items(self.sub_query, query_items))

def get_task_dependency(task: gtt.Task, dep_name: gtt.TaskName) -> gtt.Task:
    """
    From the name of a task input, obtain a task or a task ref
    """
    match task:
        case gtt.TaskNode():
            for dep in task.inputs:
                if dep.name == dep_name:
                    return dep
            raise ValueError(f'{dep_name} is not an input of {task}')
        case gtt.TaskRef():
            return gtt.TaskRef(dep_name)

def merge_query_items(sub_query, query_items) -> dict:
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

    def inner_result(self, _stat_cmd, query_items) -> Ok[dict]:
        """
        Merge argument query items
        """
        return Ok(merge_query_items(self.sub_query, query_items))

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

        return query(task, sub_query)

    def inner_result(self, _srun_cmd, subs) -> Ok[object]:
        """
        Return result of simple indexed command
        """
        item, = subs
        return Ok(item)

class Compound(Operator, named=False):
    """
    Collection of several other operators
    """
    def __init__(self, subject, sub_queries):
        super().__init__(subject, None)
        self.sub_queries = sub_queries

    def __call__(self) -> cm.Command[object]:
        return cm.collect([
                query(self.subject, sub_query) for sub_query in self.sub_queries
            ], keep_going=False
         ).then(lambda results: Ok({
             query[0]: result for query, result in zip(self.sub_queries, results)
         }))

class Iterate(Operator, named=False):
    """
    Iterate over object
    """
    requires = staticmethod(cm.srun)

    def _recurse(self, srun_result: object):
        """
        Extract raw result and build subqueries
        """
        shallow_obj = srun_result

        sub_query = self.sub_query
        sub_query_commands = []

        if isinstance(self.subject, gtt.LiteralTaskNode):
            task_nodes = {t.name: t for t in self.subject.children}
        else:
            task_nodes = {}

        if not isinstance(shallow_obj, Iterable):
            raise TypeError('Object is not a collection of tasks, cannot'
                + ' use a "*" query here')
        for task_ref in shallow_obj:
            if not isinstance(task_ref, (gtt.TaskNode, gtt.TaskRef)):
                raise TypeError('Object is not a collection of tasks, cannot'
                    + ' use a "*" query here')
            # Issue # 81
            sub_query_commands.append(
                    query(task_nodes.get(task_ref.name, task_ref),
                        sub_query)
                    )

        return sub_query_commands

    def inner_result(self, _srun_cmd, results) -> Ok[list]:
        """
        Pack items
        """
        return Ok(list(results))
