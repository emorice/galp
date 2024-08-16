"""
Reporting and printing
"""
import sys
from typing import Callable
from dataclasses import dataclass, field

import galp.task_types as gtt

class Printer:
    """
    Printer classes interface
    """
    def update_task_status(self, task_def: gtt.CoreTaskDef, done: bool | None):
        """
        Inform that a task is started (None), done (True) or failed (False)
        """
        raise NotImplementedError

    def update_task_output(self, task_def: gtt.CoreTaskDef, status: str):
        """
        Live task output
        """
        raise NotImplementedError

class PassTroughPrinter(Printer):
    """
    Trivial printer class, outputs no metadata and leaves statuses as is
    """
    def update_task_status(self, _task_def: gtt.CoreTaskDef, _done: bool | None):
        pass

    def update_task_output(self, _task_def: gtt.CoreTaskDef, status: str):
        print(status, flush=True, end='')

CTRL_UP = '\033[A;'
CTRL_RETKILL = '\r\033[K'
CTRL_RED = '\033[3qm'
CTRL_GREEN = '\033[32m'
CTRL_DEFCOL = '\033[39m'
GREEN_OK = CTRL_GREEN + 'OK' + CTRL_DEFCOL
RED_FAIL = CTRL_RED + 'FAIL' + CTRL_DEFCOL

@dataclass
class LiveDisplay:
    """Live display interface"""

    def update_log(self, log: list[str]):
        """
        Append to the log that must be displayed somewhere alongside the
        summary
        """
        raise NotImplementedError

    def update_all(self, summary: list[str], log: list[str]):
        """
        Update the summary that must be overwritten in place and the log at once
        """
        raise NotImplementedError

@dataclass
class TerminalLiveDisplay(LiveDisplay):
    """
    Display summary at bottom of screen using ansi escapes
    """
    summary: list[str] = field(default_factory=list)
    n_lines: int = 0

    def update_log(self, log: list[str]):
        # To add to the log, we need to overwrite and re-display the summary
        return self.update_all(self.summary, log)

    def update_all(self, summary: list[str], log: list[str]):
        # Save summary for update_log
        self.summary = summary

        # Go up
        print(CTRL_UP * self.n_lines, end='')

        # Print buffer
        for line in log:
            print(f'{CTRL_RETKILL}{line}')

        # Print summary
        for line in summary:
            print(f'{CTRL_RETKILL}{line}')
        # Remember how many lines we'll have to overwrite next time
        self.n_lines = len(summary)

        sys.stdout.flush()

@dataclass
class JupyterLiveDisplay(LiveDisplay):
    """
    Display summary in jupyter writable display
    """
    display_function: Callable
    handle = None

    def update_log(self, log: list[str]):
        # Do a normal print
        if log:
            print('\n'.join(log), flush=True)

    def update_all(self, summary: list[str], log: list[str]):
        self.update_log(log)
        summary_bundle = {'text/plain': '\n'.join(summary)}
        if self.handle:
            self.handle.update(summary_bundle, raw=True)
        else:
            self.handle = self.display_function(summary_bundle, raw=True, display_id=True)


@dataclass
class TaskPrinter(Printer):
    """
    Object keeping state on what tasks are running, integrating info from hooks,
    and offering flexible output.
    """
    running: dict[str, set[gtt.TaskName]] = field(default_factory=dict)
    out_lines: list[str] = field(default_factory=list)
    live_display: LiveDisplay = field(default_factory=TerminalLiveDisplay)

    def update_task_status(self, task_def: gtt.CoreTaskDef, done: bool | None):
        step = task_def.step
        name = task_def.name

        # Update state and compute log
        log_lines = []
        if done is None:
            if step not in self.running:
                self.running[step] = set()
            self.running[step].add(name)
        else:
            if step in self.running:
                if name in self.running[step]:
                    self.running[step].remove(name)
                if not self.running[step]:
                    del self.running[step]
            log_lines.append(
                    f'{step} {name} [{GREEN_OK if done else RED_FAIL}]'
                    )

        # Recompute summary
        max_lines = 10
        n_steps = len(self.running)
        summary = []
        for istep, (step, names) in enumerate(self.running.items()):
            if istep >= max_lines - 1 and n_steps > max_lines :
                summary.append(f'and {n_steps - max_lines + 1} other steps')
                break
            tasks = f'task {next(iter(names))}' if len(names) == 1 else f'{len(names)} tasks'
            summary.append(f'{step} [{tasks} pending]')

        # Display both
        self.live_display.update_all(summary, log_lines)

    def update_task_output(self, task_def: gtt.CoreTaskDef, status: str):
        out_lines = []
        for line in status.splitlines():
            out_lines.append(
                    f'{task_def.step} {task_def.name}: {line}'
                    )
        self.live_display.update_log(out_lines)


def make_printer(verbose: bool) -> Printer:
    """
    Choose a printer
    """
    if verbose:
        # Detect ipython, taken from tqdm
        ipython = sys.modules.get('IPython')
        if ipython and 'IPKernelApp' in ipython.get_ipython().config:
            return TaskPrinter(
                    live_display=JupyterLiveDisplay(
                        display_function=ipython.display.display
                        )
                    )
        return TaskPrinter(live_display=TerminalLiveDisplay())
    return PassTroughPrinter()
