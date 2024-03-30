"""
Dashboard to visualize task results
"""

import json
import logging
from importlib import import_module

from flask import Flask, render_template, abort

import galp.config
import galp.commands as cm
import galp.asyn as ga
from galp.net.core.types import Get
from galp.task_types import (TaskSerializer, TaskNode, CoreTaskDef, Serialized,
    TaskRef)
from galp.cache import CacheStack
from galp.result import Ok
from galp.query import collect_task_inputs

def render_object(obj):
    """
    Tries to smartly render the object in whatever form is the most convenient
    """

    modname = obj.__class__.__module__

    if modname.startswith('plotly'):
        # Plotly figures
        try:
            pio = import_module('plotly.io')
            return True, pio.to_html(obj)
        except ModuleNotFoundError:
            logging.warning('Found a plotly object but cannot load plotly')

    if modname.startswith('pandas'):
        if hasattr(obj, 'to_string'):
            return False, obj.to_string()
    # Anything pretty-printable as json
    try:
        return False, json.dumps(obj, indent=4)
    except TypeError:
        pass

    # Fallback to str
    return False, str(obj)

def create_app(config):
    """
    Creates the dashboard flask app from a galp.config-compatible dictionnary
    """
    app = Flask(__name__)
    app.galp = galp.config.load_config(config)

    @app.route('/')
    def landing():
        """
        Landing page of the app
        """
        return render_template('landing.html',
                steps=app.galp['steps'].all_steps
                )

    @app.route('/step/<step_name>')
    def run(step_name):
        """
        Execute a no-argument step and displays the result
        """
        steps = app.galp['steps']
        store = app.galp['store']

        if step_name not in steps:
            abort(404)

        step = steps[step_name]
        task = step()

        if step.is_view:
            # inject deps and check for missing args
            task = step()

            kwargs = collect_kwargs(store, task)

            # Run the step
            result = step.function(**kwargs)
            # Render the result
            is_safe, rep = render_object(result)
            return render_template('step.html', safe_obj=is_safe, obj=rep)

        if task.name in store:
            obj = store.get_native(task.name)
            is_safe, rep = render_object(obj)
            return render_template('step.html', safe_obj=is_safe, obj=rep)

        return 'Neither a view nor an object in store'
    return app

def collect_kwargs(store: CacheStack, task: TaskNode) -> dict:
    """
    Re-use client logic to parse the graph and sort out which pieces
    need to be fetched from store
    """
    tdef = task.task_def
    assert isinstance(tdef, CoreTaskDef)

    ## Define how to fetch missing pieces (direct read from store)
    script = cm.Script()
    def _exec(command: cm.InertCommand
              ) -> tuple[list[None], list[cm.InertCommand]]:
        """Fulfill commands by reading from stores"""
        if not isinstance(command, cm.Send):
            raise NotImplementedError(command)
        if not isinstance(command.request, Get):
            raise NotImplementedError(command)
        name = command.request.name
        res = store.get_serial(name)
        return [], script.done(command.key, Ok(res))

    ## Collect args from local and remote store. Since we don't pass any
    ## argument to the step, all arguments are injected, and therefore keyword arguments

    cmd = collect_task_inputs(task, tdef)
    primitives = script.init_command(cmd)
    unprocessed = ga.filter_commands(primitives, _exec)
    assert not unprocessed
    if not isinstance(cmd.val, Ok):
        raise NotImplementedError(cmd.val)
    args, kwargs = cmd.val.value
    assert not args

    return kwargs
