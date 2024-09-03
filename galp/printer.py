"""
Reporting and printing
"""
import os
import sys
import time
import logging
from typing import Callable
from dataclasses import dataclass
from contextlib import asynccontextmanager, AsyncExitStack

from galp.result import Ok, Error, Result
from galp.net.core.types import TaskProgress
import galp.task_types as gtt

class Printer:
    """
    Printer classes interface
    """
    def update_task_status(self, task_def: gtt.CoreTaskDef,
                           over: Result | TaskProgress | None) -> None:
        """
        Inform that a task is started (None), over (True) or failed (False)
        """
        raise NotImplementedError

    def update_task_output(self, task_def: gtt.CoreTaskDef,
                           status: TaskProgress) -> None:
        """
        Live task output
        """
        raise NotImplementedError

class PassTroughPrinter(Printer):
    """
    Trivial printer class, outputs no metadata and leaves statuses as is
    """
    def update_task_status(self, _task_def: gtt.CoreTaskDef,
                           _over: Result | TaskProgress | None):
        pass

    def update_task_output(self, _task_def: gtt.CoreTaskDef,
                           status: TaskProgress) -> None:
        match status['event']:
            case 'stdout':
                os.write(1, status['payload'])
            case 'stderr':
                os.write(2, status['payload'])
            # Else skip

CTRL_UP = '\033[A;'
CTRL_RETKILL = '\r\033[K'
# Disabled until we can exclude html output
CTRL_RED = '' # '\033[31m'
CTRL_GREEN = '' #'\033[32m'
CTRL_DEFCOL = '' #'\033[39m'
GREEN_OK = CTRL_GREEN + 'OK' + CTRL_DEFCOL
RED_FAIL = CTRL_RED + 'FAIL' + CTRL_DEFCOL

class LiveDisplay:
    """Live display interface"""

    def update_log(self, log: list[str], open_log: list[str]):
        """
        Append to the log that must be displayed somewhere alongside the
        summary. The open log are unfinished lines that may me displayed
        dynamically
        """
        raise NotImplementedError

    def update_summary(self, summary: list[str], log: list[str]):
        """
        Update the summary that must be overwritten in place
        """
        raise NotImplementedError

    async def __aenter__(self):
        """
        Called before collection
        """

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        Called after collection
        """

class LogFileDisplay(LiveDisplay):
    """Append only logs to a file named galp.log"""
    def __init__(self):
        self.file = None

    def update_log(self, log: list[str], open_log: list[str]):
        for line in log:
            print(line, file=self.file)
        self.file.flush()

    def update_summary(self, summary: list[str], log: list[str]):
        for line in log:
            print(line, file=self.file)
        self.file.flush()

    async def __aenter__(self):
        """
        Called before collection
        """
        # pylint: disable=consider-using-with
        self.file = open('galp.log', 'a', encoding='utf-8')

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        Called after collection
        """
        self.file.close()

class TerminalLiveDisplay(LiveDisplay):
    """
    Display summary at bottom of screen using ansi escapes
    """
    def __init__(self) -> None:
        self.summary: list[str] = []
        self.open_log: list[str] = []
        self.n_lines: int = 0

    def update_log(self, log: list[str], open_log: list[str]):
        # Save open log for update_summary
        self.open_log = open_log
        # To add to the log, we need to overwrite and re-display all
        return self._display_all(log)

    def update_summary(self, summary: list[str], log: list[str]):
        # Save summary for update_log
        self.summary = summary
        return self._display_all(log)

    def _display_all(self, log: list[str]):
        # Go up and clear
        print((CTRL_UP + CTRL_RETKILL) * self.n_lines, end='')

        # Print buffer
        for line in log:
            print(line)

        # Print summary and open log
        dyn_lines = self.summary + self.open_log
        # Print backwards, most important line last
        for line in reversed(dyn_lines):
            print(line)
        # Remember how many lines we'll have to overwrite next time
        self.n_lines = len(dyn_lines)

        sys.stdout.flush()

@dataclass
class JupyterLiveDisplay(LiveDisplay):
    """
    Display summary in jupyter writable display
    """
    display_function: Callable
    handles = None

    def update_log(self, log: list[str], open_log: list[str]):
        # Do a normal print for log
        if log:
            print('\n'.join(log), flush=True)
        self._update_display(1, ['Recent output from running tasks:', *open_log,
                                 ' '])

    def update_summary(self, summary: list[str], log: list[str]):
        # Do a normal print for log
        if log:
            print('\n'.join(log), flush=True)
        self._update_display(0, ['Tasks summary:', *summary, ' '])

    def _update_display(self, handle_pos: int, lines: list[str]):
        display_bundle = {'text/plain': '\n'.join(lines)}
        if self.handles:
            self.handles[handle_pos].update(display_bundle, raw=True)
        else:
            self.handles = [
                    self.display_function(
                        display_bundle if i == handle_pos
                        else {'text/plain': ''},
                        raw=True, display_id=True)
                    for i in (0, 1)
                    ]
            print('Execution log:', flush=True)

HTML_CONTENT = """
<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8" />
        <script src="https://code.jquery.com/jquery-3.7.1.min.js"
              integrity="sha256-/JqT3SQfawRcv/BIHPThkBvs0OEvtFFmqPF/lYI/Cxo="
			  crossorigin="anonymous"></script>
        <script>
            // Dynamic url to handle running behind  jupyter-server-proxy
            const content_url = window.location.href.replace(/\\/$/, '') + '/content'
            function load() {
                $.ajax({
                    'url': content_url,
                    'success': data => {
                            $('#summary').text(data.summary);
                            $('#open').text(data.open);
                            $('#closed').text(data.closed);
                            $('#status').text('Server online, updating every 5 seconds...');
                            setTimeout(load, 5000);
                        },
                    'error': () => $('#status').text('\u26a0 Server disconnected'),
                    'dataType': 'json',
                    });
                }
            load();
        </script>
        <style>
            body {
                font-family: sans-serif;
                max-width: 1024px;
                margin: auto;
            }
            pre {
                background: #e0e0e0;
                border-radius: 5px;
                padding: 8px;
                margin-top: 8px;
                margin-bottom: 8px;
                overflow-x: scroll;
                }
        </style>
        <title>Pipeline run live summary</title>
    </head>
    <body>
    <h1>Pipeline run live summary</h1>
    <p id="status">Connecting...</p>
    <span>Tasks summary:</span>
    <pre id="summary"></pre>
    <span>Recent output from running tasks:</span>
    <pre id="open"></pre>
    <span>Execution log:</span>
    <pre id="closed"></pre>
    </body>
</html>
"""

class HTTPLiveDisplay(LiveDisplay):
    """
    Start an http server that display progress on demand
    """
    def __init__(self) -> None:
        self.summary: list[str] = []
        self.open_log: list[str] = []
        self.log: list[str] = []
        self.max_lines = 10_000

        # pylint: disable=import-outside-toplevel # optdepend
        try:
            from aiohttp import web
        except ImportError as exc:
            raise TypeError('aiohttp is needed for output=\'http\'') from exc
        self.web = web
        app = web.Application()
        app.add_routes([
            web.get('/', self.display),
            web.get('/content', self.content),
            ])
        self.runner = web.AppRunner(app)
        self.site = None

    async def __aenter__(self):
        """
        Start serving display requests
        """
        await self.runner.setup()
        site = self.web.TCPSite(self.runner, 'localhost', 0)
        await site.start()
        # pylint: disable=protected-access
        host, port, *extra = site._server.sockets[0].getsockname()
        print(f'Serving locally on http://{host}:{port}')
        if extra:
            print(extra)
        print(f"""\
If port forwarding is required, make sure jupyter-server-proxy is installed,
and use e.g. <jupyter lab url>/proxy/{port} with the url you currently see in
your address bar, e.g. http://localhost:8888/proxy/{port}""", flush=True)


    async def display(self, request):
        """
        Callback for page template
        """
        del request
        return self.web.Response(
                content_type='text/html',
                body=HTML_CONTENT
                )

    async def content(self, request):
        """"
        Callback for page content
        """
        del request
        return self.web.json_response({
            'summary': '\n'.join(self.summary),
            'open': '\n'.join(self.open_log),
            'closed': '\n'.join(self.log),
            })

    def update_log(self, log: list[str], open_log: list[str]):
        self.log = (self.log + log)[:self.max_lines]
        self.open_log = open_log

    def update_summary(self, summary: list[str], log: list[str]):
        self.log = (self.log + log)[:self.max_lines]
        self.summary = summary

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        Stop serving display requests
        """
        await self.runner.cleanup()

def emulate_cr(string: str):
    """
    Emulate \r effect
    """
    return string.rpartition('\r')[-1]

def hour() -> str:
    """Local hour as string"""
    ltime = time.localtime()
    return f'{ltime.tm_hour:02d}:{ltime.tm_min:02d}'

@dataclass
class _TaskSets:
    pending: set[gtt.TaskName]
    running: set[gtt.TaskName]
    done: set[gtt.TaskName]
    failed: set[gtt.TaskName]

class TaskPrinter(Printer):
    """
    Object keeping state on what tasks are running, integrating info from hooks,
    and offering flexible output.
    """

    def __init__(self, live_displays: list[LiveDisplay]) -> None:
        self.tasks: dict[str, _TaskSets] = {}
        self.out_lines: list[str] = []
        self.live_displays = live_displays
        # Key is the header "<step> <name>", value is (time, text)
        self.open_lines: dict[str, tuple[str, str]] = {}

    def update_task_status(self, task_def: gtt.CoreTaskDef,
                           over: Result | TaskProgress | None):
        step = task_def.step
        name = task_def.name
        ltime = hour()


        # Update state and compute log
        log_lines = []
        if step not in self.tasks:
            self.tasks[step] = _TaskSets(set(), set(), set(), set())
        tasksets = self.tasks[step]

        if over is None:
            tasksets.pending.add(name)
        elif isinstance(over, dict) and over.get('event') == 'started':
            tasksets.pending.discard(name)
            tasksets.running.add(name)
            log_lines.append(
                    f'{ltime} {step} {name} [STARTED]'
                    )
        else: # Result
            tasksets.pending.discard(name)
            tasksets.running.discard(name)
            self.open_lines.pop(f'{step} {name}', '')
            match over:
                case Ok():
                    message = f'[{GREEN_OK}]'
                    tasksets.done.add(name)
                case Error():
                    message = f'[{RED_FAIL}] {over.error}'
                    tasksets.failed.add(name)
            log_lines.append(
                    f'{ltime} {step} {name} {message}'
                    )

        # Recompute summary
        #max_lines = 10
        #n_steps = len(self.tasks)
        summary = []
        max_step_len = 0
        for step in self.tasks:
            step_len = len(step)
            if max_step_len < step_len <= 50:
                max_step_len = step_len
        for step, sets in self.tasks.items():
            # this should be only for terminal, on web/juyter we have more space
            #if istep >= max_lines - 1 and n_steps > max_lines :
            #    summary.append(f'and {n_steps - max_lines + 1} other steps')
            #    break
            npd = len(sets.pending)
            nrun = len(sets.running)
            ndone = len(sets.done)
            nfail = len(sets.failed)
            ntot = npd + ndone + nfail + nrun
            failures = f'|{nfail} FAILED' if nfail else ''
            summary.append(
                    f'{ltime} {step:{max_step_len}}    '
                    f'[R: {nrun}|P: {npd}|OK: {ndone}/{ntot}{failures}]'
                    )

        # Display both
        for display in self.live_displays:
            display.update_summary(summary, log_lines)

    def update_task_output(self, task_def: gtt.CoreTaskDef, status: TaskProgress):
        tid = f'{task_def.step} {task_def.name}'
        ltime = hour()
        # Not splitlines, we want a final empty string
        lines = status['payload'].decode('utf8', errors='ignore').split('\n')
        # Join previous hanging data
        # Don't delete the item to keep the pos in the dict
        _old_time, last_open = self.open_lines.get(tid, (None, ''))
        lines[0] = last_open + lines[0]

        # Full lines to append to log
        closed_lines = [
            f'{ltime} {tid} {emulate_cr(line)}'
            for line in lines[:-1]
            ]

        # Save final (often empty) open line, clearing previous
        self.open_lines[tid] = ltime, emulate_cr(lines[-1])

        # Open lines to display
        open_lines = [
            f'{other_ltime} {other_tid} {other_line}'
            for other_tid, (other_ltime, other_line) in self.open_lines.items()
            ]
        for display in self.live_displays:
            display.update_log(closed_lines, open_lines)

def _validate(verbose: bool, output: str) -> set[str]:
    if verbose:
        if output:
            raise TypeError('Give either verbose or output')
        logging.warning('Use output=\'auto\' instead of verbose=True')
        output = 'auto'

    outputs = set(output.split('+'))
    for out in outputs:
        if out not in ('', 'auto', 'http', 'ipython', 'console', 'logfile'):
            raise TypeError(f'No such output: {out}')

    return outputs

@asynccontextmanager
async def make_printer(verbose: bool, output: str):
    """
    Choose a printer
    """
    outputs = _validate(verbose, output)
    displays: list[LiveDisplay] = []


    ipython = sys.modules.get('IPython')

    # Resolve auto
    if 'auto' in outputs:
        # Detect ipython, taken from tqdm
        in_ipython = ipython and 'IPKernelApp' in ipython.get_ipython().config
        outputs.remove('auto')
        outputs.add('ipython' if in_ipython else 'console')

    if 'console' in outputs:
        displays.append(TerminalLiveDisplay())

    if 'ipython'in outputs:
        if not ipython:
            raise RuntimeError('IPython not loaded')
        displays.append(JupyterLiveDisplay(
            display_function=ipython.display.display
            ))

    if 'http' in outputs:
        displays.append(HTTPLiveDisplay())

    if 'logfile' in outputs:
        displays.append(LogFileDisplay())

    stack = AsyncExitStack()
    for display in displays:
        await stack.enter_async_context(display)

    async with stack:
        if displays:
            yield TaskPrinter(live_displays=displays)
        else:
            yield PassTroughPrinter()
