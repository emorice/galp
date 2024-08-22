"""
Reporting and printing
"""
import sys
import logging
from typing import Callable
from dataclasses import dataclass, field
from contextlib import asynccontextmanager, AsyncExitStack

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
        self._update_display(1, open_log)

    def update_summary(self, summary: list[str], log: list[str]):
        # Do a normal print for log
        if log:
            print('\n'.join(log), flush=True)
        self._update_display(0, summary)

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

HTML_CONTENT = """
<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8" />
        <script src="https://code.jquery.com/jquery-3.7.1.min.js"
              integrity="sha256-/JqT3SQfawRcv/BIHPThkBvs0OEvtFFmqPF/lYI/Cxo="
			  crossorigin="anonymous"></script>
        <script>
            function load() {
                $.ajax({
                    'url': '/content',
                    'success': data => {
                            $('#container').text(data);
                            $('#status').text('Server online, updating every 5 seconds');
                            setTimeout(load, 5000);
                        },
                    'error': () => $('#status').text('\u26a0 Server disconnected'),
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
        </style>
        <title>Pipeline run live summary</title>
    </head>
    <body>
    <h1>Pipeline run live summary</h1>
    <div id="status">Connecting...</div>
    <pre id="container"></pre></body>
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
        host, port = site._server.sockets[0].getsockname()
        print(f'Serving on http://{host}:{port}', flush=True)


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
        return self.web.Response(
                text='\n'.join(
                    self.summary
                    + self.open_log
                    + self.log
                    ),
                )

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

@dataclass
class TaskPrinter(Printer):
    """
    Object keeping state on what tasks are running, integrating info from hooks,
    and offering flexible output.
    """
    running: dict[str, set[gtt.TaskName]] = field(default_factory=dict)
    out_lines: list[str] = field(default_factory=list)
    live_displays: list[LiveDisplay] = field(default_factory=list)
    # Key is the header "<step> <name>"
    open_lines: dict[str, str] = field(default_factory=dict)

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
                self.open_lines.pop(f'{step} {name}', '')
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
        for display in self.live_displays:
            display.update_summary(summary, log_lines)

    def update_task_output(self, task_def: gtt.CoreTaskDef, status: str):
        header = f'{task_def.step} {task_def.name}'
        # Not splitlines, we want a final empty string
        lines = status.split('\n')
        # Join previous hanging data
        # Don't delete the item to keep the pos in the dict
        lines[0] = self.open_lines.get(header, '') + lines[0]

        # Full lines to append to log
        closed_lines = [
            f'{header} {emulate_cr(line)}'
            for line in lines[:-1]
            ]

        # Save final (often empty) open line, clearing previous
        self.open_lines[header] = emulate_cr(lines[-1])

        # Open lines to update
        open_lines = [
            f'{other_header} {other_line}'
            for other_header, other_line in self.open_lines.items()
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
