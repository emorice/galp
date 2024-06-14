"""
Lists of internal commands
"""

from typing import Hashable, TypeVar, Callable
from dataclasses import dataclass
from weakref import WeakValueDictionary

import galp.net.requests.types as gr
import galp.net.core.types as gm
import galp.task_types as gtt
import galp.asyn as ga

from galp.result import Result, Ok, Error
from galp.asyn import Command, collect, Primitive, CommandLike

# Custom Script
# -------------

class Script(ga.Script):
    """
    Override script hook with domain-specific logger

    Args:
        verbose: whether to print brief summary of command completion to stderr
    """
    def __init__(self, verbose: bool = True) -> None:
        self.verbose = verbose
        super().__init__()

# Primitives
# ----------

T_co = TypeVar('T_co', covariant=True)

class Send(Primitive[T_co]):
    """Send an arbitrary request"""
    request: gm.Request
    def __init__(self, request: gm.BaseRequest[T_co]):
        super().__init__()
        assert isinstance(request, gm.Request) # type: ignore # bug
        self.request = request # type: ignore # guarded by assertion

    @property
    def key(self) -> Hashable:
        return gm.get_request_id(self.request)

class End(Primitive):
    """Finish the command processing"""
    def __init__(self, value):
        super().__init__()
        self.value = value

    # This marks the command as output-style
    @property
    def key(self) -> Hashable:
        return None

# Parameters
# ----------

@dataclass
class ExecOptions:
    """
    Collection of parameters that affect how a graph should be executed

    Attributes:
        dry: whether to perform a dry-run or real run
        keep_going: whether to continue executing independent branches of the
            graph after a failure
        resources: how much resources to require for each task
    """
    dry: bool = False
    keep_going: bool = False
    resources: gtt.ResourceClaim = gtt.ResourceClaim(cpus=1)

# Routines
# --------

U = TypeVar('U')
V = TypeVar('V')
def recursive_async_cache(function: Callable[[U, Callable[[U], CommandLike[V]]], CommandLike[V]]
        ) -> Callable[[U], CommandLike[V]]:
    """Recursive asynchronous cache.

    Wraps the asynchronous function to insert a local cache lookup. The wrapped
    function is injected as the last argument of itself, to allow the function
    to recurse. This allows to build correct asynchronous algorithms to act on
    directed acyclic graphs.
    """
    cache: dict[U, V] = {}
    def _cacheit(arg: U) -> Callable[[V], Ok[V]]:
        def _inner(obj: V) -> Ok[V]:
            cache[arg] = obj
            return Ok(obj)
        return _inner
    def wrapper(arg: U) -> CommandLike[V]:
        try:
            return Ok(cache[arg])
        except KeyError:
            return function(arg, wrapper).then(_cacheit(arg))
    return wrapper

def send_get(name: gtt.TaskName) -> Command[gtt.Serialized]:
    """Deduplicated get primitive creation"""
    return Send(gm.Get(name))

def fetch(task: gtt.Task) -> CommandLike[gtt.Serialized]:
    """
    Get an object but do not deserialize it yet.

    Includes bypass for Literals.
    """
    # Shortcut for literals
    if isinstance(task, gtt.LiteralTaskNode):
        return Ok(task.serialized)

    return send_get(task.name)

def _inner_rget(task: gtt.Task,
        next_rget: Callable[[gtt.Task], CommandLike[object]]
        ) -> CommandLike[object]:
    return (
        fetch(task)
        .then(lambda res: (
            collect([next_rget(c) for c in res.children], keep_going=False)
            .then(res.deserialize)
            ))
        )

def rget(task: gtt.Task) -> CommandLike[object]:
    """
    Get a task result, then recursively get all the sub-parts of it

    This unconditionally fails if a sub-part fails.
    """
    # Creates a one-off cache
    _rget = recursive_async_cache(_inner_rget)
    # Makes recursive calls that share said cache
    return _rget(task)

def sget(task: gtt.Task) -> CommandLike[object]:
    """
    Shallow or simple get: get a task result, and deserialize it but keeping
    children as references instead of recursing on them like rget
    """
    return (
        fetch(task)
        .then(lambda res: res.deserialize(res.children))
        )

def _no_not_found(stat_result: gr.StatReplyValue, task: gtt.Task
                 ) -> Result[gr.Found | gr.StatDone]:
    """
    Transform NotFound in Found if the task is a real object, and fails
    otherwise.
    """
    if not isinstance(stat_result, gr.NotFound):
        return Ok(stat_result)

    if isinstance(task, gtt.TaskNode):
        return Ok(gr.Found(task_def=task.task_def))

    return Error(f'The task reference {task.name} could not be resolved to a'
        ' definition')

def safe_stat(task: gtt.Task) -> Command[gr.StatDone | gr.Found]:
    """
    Chains no_not_found to a stat
    """
    return (
            Send(gm.Stat(task.name))
            .then(lambda statr: _no_not_found(statr, task))
          )

# Note: the default exec options is just for compat with Query and to be removed
# once galp.query gets re-written more flexibly

def ssubmit(task: gtt.Task, options: ExecOptions = ExecOptions()
            ) -> Command[gtt.ResultRef]:
    """
    A non-recursive ("simple") task submission: executes dependencies, but not
    children. Return said children as result on success.
    """
    return safe_stat(task).then(lambda statr: _ssubmit(task, statr, options))

def _ssubmit(task: gtt.Task, stat_result: gr.Found | gr.StatDone,
             options: ExecOptions) -> CommandLike[gtt.ResultRef]:
    """
    Core ssubmit logic, recurse on dependencies and skip done tasks
    """
    # Short circuit for tasks already processed
    if isinstance(stat_result, gr.StatDone):
        return Ok(stat_result.result)

    # gm.Found()
    task_def = stat_result.task_def

    # Short circuit for literals
    if isinstance(task_def, gtt.LiteralTaskDef):
        # Issue #78: at this point we should be making sure children are saved
        # before handing back references to them
        match task:
            case gtt.TaskRef():
                # If we have a reference to a literal, we assume the children
                # definitions have been saved and we can refer to them
                return Ok(gtt.ResultRef(task.name,
                        list(map(gtt.TaskRef, task_def.children))
                        ))
            case gtt.LiteralTaskNode():
                # Else, we have a literal that wasn't found on the remote,
                # upload it.
                # We still return a full ref, not a flat ref,
                # because we do have the child tasks available locally,
                # but maybe not remotely yet
                return (
                        Send(gm.Upload(task_def, task.serialized))
                        .then(
                            lambda _flat_ref: Ok(gtt.ResultRef(
                                task.name, task.dependencies
                                ))
                            )
                        )
            case _:
                assert False, 'Bad task type'

    # Query, should never have reached this layer as queries have custom run
    # mechanics
    if isinstance(task_def, gtt.QueryTaskDef):
        raise NotImplementedError

    # Finally, Core or Child, process dependencies/parent first

    # Collect dependencies
    deps: list[gtt.Task]
    if isinstance(task, gtt.TaskRef):
        # If a reference, by design the dep defs have been checked in
        deps = [gtt.TaskRef(tin.name)
                for tin in task_def.dependencies(gtt.TaskOp.BASE)]
    else:
        # If a node, we have the defs and pass them directly
        deps = task.dependencies

    # Issue 79: optimize type of command based on type of link
    # Always doing a RSUB will work, but it will run things more eagerly that
    # needed or propagate failures too aggressively.
    gather_deps = collect([rsubmit(dep, options) for dep in deps],
                         options.keep_going)

    # Issue final command
    if options.dry or isinstance(task_def, gtt.ChildTaskDef):
        return gather_deps.then(lambda _: Ok(gtt.ResultRef(task.name, [])))

    return (
            gather_deps
            .then(lambda _: Send(gm.Submit(task_def, options.resources)))
            )

def rsubmit(task: gtt.Task, options: ExecOptions
            ) -> Command[gtt.RecResultRef]:
    """
    Recursive submit, with children, i.e a ssubmit plus a rsubmit per child

    For dry-runs, does not actually submit tasks, and instead moves on as soon
    as the STAT command succeeded,

    If a task has dynamic children (i.e., running the task returns new tasks to
    execute), the output will depend on the task status:
     * If the task is done, the dry-run will be able to list the child tasks and
       recurse on them.
     * If the task is not done, the dry-run will not see the child tasks and
       skip them, so only the parent task will show up in the dry-run log.

    This is the intended behavior, it reflects the inherent limit of a dry-run:
    part of the task graph is unknown before we actually start executing it.
    """
    return (
            ssubmit(task, options)
            .then(lambda res: collect(
                [rsubmit(c, options) for c in res.children],
                options.keep_going)
                  .then(lambda child_results: Ok(
                      gtt.RecResultRef(res, child_results)
                      ))
                  )
            )

def run(task: gtt.Task, options: ExecOptions) -> Command[object]:
    """
    Combined rsubmit + rget

    If dry, just a dry rsubmit
    """
    if options.dry:
        return rsubmit(task, options)
    return rsubmit(task, options).then(lambda _: rget(task))

# Note: the default exec options is just for compat with Query and to be removed
# once galp.query gets re-written more flexibly

def srun(task: gtt.Task, options: ExecOptions = ExecOptions()
         ) -> Command[object]:
    """
    Shallow run: combined ssubmit + sget, fetches the raw result of a task but
    not its children
    """
    return ssubmit(task, options).then(lambda _: sget(task))
