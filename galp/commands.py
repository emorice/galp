"""
Lists of internal commands
"""

import sys
import time
from typing import Sequence, Hashable, TypeVar

import galp.net.requests.types as gr
import galp.net.core.types as gm
import galp.task_types as gtt
import galp.asyn as ga

from galp.result import Ok, Error
from galp.serialize import LoadError
from galp.asyn import Command, Gather, InertCommand

class Script(ga.Script):
    """
    Override script hook with domain-specific logger
    """
    def notify_change(self, command: Command, old_value: ga.State,
                      new_value: ga.State) -> None:
        """
        Hook called when the graph status changes
        """
        if not self.verbose:
            return
        del old_value

        def print_status(stat, name, step_name):
            print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} {stat:4} [{name}]'
                    f' {step_name}',
                    file=sys.stderr, flush=True)

        if not isinstance(command, Send):
            return
        req = command.request

        if isinstance(req, gm.Stat):
            if isinstance(new_value, Ok):
                task_done, task_def, _children = new_value.value
                if not task_done and isinstance(self, gtt.CoreTaskDef):
                    print_status('PLAN', req.name, task_def.step)

        if isinstance(req, gm.Submit):
            step_name_s = req.task_def.step
            name = req.task_def.name

            # We log 'SUB' every time a submit req's status "changes" to
            # PENDING. Currently this only happens when the req is
            # created.
            match new_value:
                case ga.Pending():
                    print_status('SUB', name, step_name_s)
                case Ok():
                    print_status('DONE', name, step_name_s)
                case Error():
                    print_status('FAIL', name, step_name_s)


class Submit(InertCommand[gtt.ResultRef, Error]):
    """
    Remotely execute a single step
    """
    def __init__(self, task_def: gtt.CoreTaskDef):
        self.task_def = task_def
        super().__init__()

    @property
    def key(self):
        return (self.__class__.__name__.upper(), self.task_def.name)

    @property
    def name(self):
        """Task name"""
        return self.task_def.name


T = TypeVar('T')

class Send(InertCommand[T, Error]):
    """Send an arbitrary request"""
    request: gm.Request
    def __init__(self, request: gm.BaseRequest[T]):
        super().__init__()
        assert isinstance(request, gm.Request) # type: ignore # bug
        self.request = request # type: ignore # guarded by assertion

    @property
    def key(self) -> Hashable:
        return gm.get_request_id(self.request).as_legacy_key()

def safe_deserialize(res: gr.Put, children: Sequence):
    """
    Wrap serializer in a guard for invalid payloads
    """
    match res.deserialize(children):
        case LoadError() as err:
            return err
        case Ok(result):
            return result

def rget(name: gtt.TaskName) -> Command[object, Error]:
    """
    Get a task result, then rescursively get all the sub-parts of it
    """
    return (
        Send(gm.Get(name))
        .then(lambda res: (
            Gather([rget(c.name) for c in res.children])
            .then(lambda children: safe_deserialize(res, children))
            ))
        )

def sget(name: gtt.TaskName) -> Command[object, Error]:
    """
    Shallow or simple get: get a task result, and deserialize it but keeping
    children as references instead of recursing on them like rget
    """
    return (
        Send(gm.Get(name))
        .then(lambda res: safe_deserialize(res, res.children))
        )

def no_not_found(stat_result: gr.StatReplyValue, task: gtt.Task
                 ) -> gr.Found | gr.StatDone | Error:
    """
    Transform NotFound in Found if the task is a real object, and fails
    otherwise.
    """
    if not isinstance(stat_result, gr.NotFound):
        return stat_result

    if isinstance(task, gtt.TaskNode):
        return gr.Found(task_def=task.task_def)

    return Error(f'The task reference {task.name} could not be resolved to a'
        ' definition')

def safe_stat(task: gtt.Task) -> Command[gr.StatDone | gr.Found, Error]:
    """
    Chains no_not_found to a stat
    """
    return (
            Send(gm.Stat(task.name))
            .then(lambda statr: no_not_found(statr, task))
          )

def ssubmit(task: gtt.Task, dry: bool = False
            ) -> Command[gtt.ResultRef, Error]:
    """
    A non-recursive ("simple") task submission: executes dependencies, but not
    children. Return said children as result on success.
    """
    return safe_stat(task).then(lambda statr: _ssubmit(task, statr, dry))

def _ssubmit(task: gtt.Task, stat_result: gr.Found | gr.StatDone, dry: bool
             ) -> gtt.ResultRef | Command[gtt.ResultRef, Error]:
    """
    Core ssubmit logic, recurse on dependencies and skip done tasks
    """
    # Short circuit for tasks already processed
    if isinstance(stat_result, gr.StatDone):
        return stat_result.result

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
                return gtt.ResultRef(task.name,
                        list(map(gtt.TaskRef, task_def.children))
                        )
            case gtt.TaskNode():
                # Else, we can't hand out references but we have the true tasks
                # instead
                return gtt.ResultRef(task.name, task.dependencies)

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
    gather_deps = Gather([rsubmit(dep, dry) for dep in deps])

    # Issue final command
    if dry or isinstance(task_def, gtt.ChildTaskDef):
        return gather_deps.then(lambda _: gtt.ResultRef(task.name, []))

    return (
            gather_deps
            .then(lambda _: Submit(task_def)) # type: ignore[arg-type] # False positive
            )

def rsubmit(task: gtt.Task, dry: bool = False) -> Command[gtt.RecResultRef, Error]:
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
            ssubmit(task, dry)
            .then(lambda res: Gather([rsubmit(c, dry) for c in res.children])
                .then(
                    lambda child_results: gtt.RecResultRef(res, child_results)
                    )
                )
            )

def run(task: gtt.Task, dry=False) -> Command[object, Error]:
    """
    Combined rsubmit + rget

    If dry, just a dry rsubmit
    """
    if dry:
        return rsubmit(task, dry=True)
    return rsubmit(task).then(lambda _: rget(task.name))

def srun(task: gtt.Task):
    """
    Shallow run: combined ssubmit + sget, fetches the raw result of a task but
    not its children
    """
    return ssubmit(task).then(lambda _: sget(task.name))
