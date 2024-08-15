"""
Lists of internal commands
"""

from typing import TypeVar, Callable
from collections.abc import Hashable
from dataclasses import dataclass

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
    """
    dry: bool = False
    keep_going: bool = False
    verbose: bool = False

# Routines
# --------

U = TypeVar('U', bound=Hashable)
V = TypeVar('V')
def recursive_async_cache(function: Callable[[U, Callable[[U], CommandLike[V]]], CommandLike[V]]
        ) -> Callable[[U], CommandLike[V]]:
    """Recursive asynchronous cache.

    Wraps the asynchronous function to insert a local cache lookup. The wrapped
    function is injected as the last argument of itself, to allow the function
    to recurse. This allows to build correct asynchronous algorithms to act on
    directed acyclic graphs.
    """
    cache: dict[U, CommandLike[V]] = {}
    def _cacheit(arg: U) -> Callable[[Result[V]], Result[V]]:
        def _inner(res: Result[V]) -> Result[V]:
            cache[arg] = res
            return res
        return _inner
    def wrapper(arg: U) -> CommandLike[V]:
        try:
            return cache[arg]
        except KeyError:
            command = function(arg, wrapper)
            # Already put the command in the cache, so that overlapping async
            # calls don't create it twice
            cache[arg] = command
            # Also add a callback to eventually overwrite the command with its
            # result. It's not strictly necessary but allows the now useless
            # command memory to be reclaimed.
            return command.eventually(_cacheit(arg))
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
            ) -> CommandLike[gtt.ResultRef]:
    """
    Caching wrapper around _inner_submit, to handle correctly diamond
    dependencies
    """
    def _inner(next_task: gtt.Task,
               next_ssubmit: Callable[[gtt.Task], CommandLike[gtt.ResultRef]]
               ) -> CommandLike[gtt.ResultRef]:
        return _inner_ssubmit(next_task, options, next_ssubmit)
    caching_ssubmit = recursive_async_cache(_inner)
    return caching_ssubmit(task)

def _inner_ssubmit(task: gtt.Task, options: ExecOptions,
                  next_ssubmit: Callable[[gtt.Task], CommandLike[gtt.ResultRef]]
            ) -> Command[gtt.ResultRef]:
    """
    A non-recursive ("simple") task submission: executes dependencies, but not
    children. Return said children as result on success.
    """
    return safe_stat(task).then(
            lambda statr: _ssubmit(task, statr, options, next_ssubmit)
            )

def _upload(task: gtt.Task, stat_result: gr.Found | gr.StatDone
        ) -> CommandLike[gtt.ResultRef]:
    """
    Upload logic, which is the substitute for submit for literals
    """
    task_def = stat_result.task_def
    assert isinstance(task_def, gtt.LiteralTaskDef)

    match task:
        case gtt.TaskRef():
            # Reference to something that turned out to be a literal. This means
            # a remote literal, stat_result must be Done, and we can safely hand
            # out the result ref which may contain further valid task refs.
            assert isinstance(stat_result, gr.StatDone)
            return Ok(stat_result.result)
        case gtt.LiteralTaskNode():
            # Local literal. We still return a full ref, not a flat ref,
            # because we do have the child tasks available locally,
            # but maybe not remotely yet, and we want to keep local information.
            result_ref = gtt.ResultRef(task.name, tuple(task.dependencies))
            match stat_result:
                case gr.StatDone():
                    # Someone already did the upload. We can cut to returning
                    # the ref.
                    return Ok(result_ref)
                case gr.Found():
                    # We hit a NotFound, that was upgraded. Do the upload, but
                    # discard and upgrade the FlatRef
                    return Send(
                                gm.Upload(task_def, task.serialized)
                            ).then(
                                lambda _flat_ref: Ok(result_ref)
                            )
                case _:
                    assert False, 'Bad stat result type'
        case _:
            assert False, 'Bad task type'

def _ssubmit(task: gtt.Task, stat_result: gr.Found | gr.StatDone,
             options: ExecOptions,
             next_ssubmit: Callable[[gtt.Task], CommandLike[gtt.ResultRef]]
             ) -> CommandLike[gtt.ResultRef]:
    """
    Core ssubmit logic, recurse on dependencies and skip done tasks
    """
    task_def = stat_result.task_def

    # Specialization for literals
    if isinstance(task_def, gtt.LiteralTaskDef):
        return _upload(task, stat_result)

    # Short circuit for tasks already processed
    if isinstance(stat_result, gr.StatDone):
        return Ok(stat_result.result)

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
                for tin in task_def.dependencies]
    else:
        # If a node, we have the defs and pass them directly
        deps = task.dependencies

    # Issue 79: optimize type of command based on type of link
    # Always doing a RSUB will work, but it will run things more eagerly that
    # needed or propagate failures too aggressively.
    gather_deps = collect([rsubmit(dep, options, next_ssubmit) for dep in deps],
                         options.keep_going)

    # Issue final command
    if options.dry or isinstance(task_def, gtt.ChildTaskDef):
        return gather_deps.then(lambda _: Ok(gtt.ResultRef(task.name, tuple())))

    return (
            gather_deps
            .then(lambda _: _start_submit(task_def, options))
            )

def _end_submit(task_def: gtt.CoreTaskDef, submit_result: Result[gtt.ResultRef],
        options: ExecOptions) -> Result[gtt.ResultRef]:
    """
    Hook to report end of task
    """
    if options.verbose:
        status = 'Unknown'
        match submit_result:
            case Ok():
                status = 'Done'
            case Error():
                status = 'Failed'
        print(task_def.name, task_def.step, status)
    return submit_result

def _progress_submit(task_def: gtt.CoreTaskDef, status: str, options:
                     ExecOptions) -> None:
    if options.verbose:
        print(task_def.name, task_def.step, status, end='')
    else: # Default "pass-through" mode
        print(status, end='')

def _start_submit(task_def: gtt.CoreTaskDef, options: ExecOptions
        ) -> Command[gtt.ResultRef]:
    """
    Wrapper around sending submit that reports start of tasks and schedule
    reporting end of task.
    """
    if options.verbose:
        print(task_def.name, task_def.step, 'Submitted', flush=True)
    return Send(gm.Submit(task_def)).on_progress(
            lambda status: _progress_submit(task_def, status, options)
            ).eventually(
            lambda sub_result: _end_submit(task_def, sub_result, options)
            )


def rsubmit(task: gtt.Task, options: ExecOptions,
            ssubmit_function: Callable[[gtt.Task], CommandLike[gtt.ResultRef]] | None
            = None) -> CommandLike[gtt.RecResultRef]:
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
    if ssubmit_function is None:
        def _inner(next_task: gtt.Task,
                   next_ssubmit: Callable[[gtt.Task], CommandLike[gtt.ResultRef]]
                   ) -> CommandLike[gtt.ResultRef]:
            return _inner_ssubmit(next_task, options, next_ssubmit)
        ssubmit_function = recursive_async_cache(_inner)

    return (
            ssubmit_function(task)
            .then(lambda res: collect(
                [rsubmit(c, options, ssubmit_function) for c in res.children],
                options.keep_going)
                  .then(lambda child_results: Ok(
                      gtt.RecResultRef(res, child_results)
                      ))
                  )
            )

def run(task: gtt.Task, options: ExecOptions) -> CommandLike[object]:
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
         ) -> CommandLike[object]:
    """
    Shallow run: combined ssubmit + sget, fetches the raw result of a task but
    not its children
    """
    return ssubmit(task, options).then(lambda _: sget(task))
