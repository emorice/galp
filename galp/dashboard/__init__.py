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
from galp.graph import load_step_by_key
from galp.store import Store
from galp.result import Result, Ok
from galp.utils import collect_args

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

def create_app(endpoints: dict[str, TaskNode], store: str) -> Flask:
    """
    Creates the dashboard flask app from a galp.config-compatible dictionnary
    """
    app = Flask(__name__)
    app.galp = { # type: ignore[attr-defined]
            'endpoints': endpoints,
            'store': Store(store, TaskSerializer)
            }

    @app.route('/')
    def landing():
        """
        Landing page of the app
        """
        return render_template('landing.html',
                steps=app.galp['endpoints']
                )

    @app.route('/step/<task_name>')
    def run(task_name):
        """
        Execute a no-argument step and displays the result
        """
        endpoints = app.galp['endpoints']
        store = app.galp['store']

        if task_name not in endpoints:
            logging.error('No such endpoint: %s', task_name)
            logging.debug('Available: %s', list(endpoints))
            abort(404)

        task = endpoints[task_name]

        if task.name in store:
            obj = store.get_native(task.name)
            is_safe, rep = render_object(obj)
            return render_template('step.html', safe_obj=is_safe, obj=rep)

        # If not in store, maybe execute it locally
        task_def = task.task_def
        if isinstance(task_def, CoreTaskDef):
            step = load_step_by_key(task_def.step)
            if step.is_view:
                args, kwargs = collect_args(store, task, task_def)

                # Run the step
                result = step.function(*args, **kwargs)
                # Render the result
                is_safe, rep = render_object(result)
                return render_template('step.html', safe_obj=is_safe, obj=rep)

        return 'Neither a view nor an object in store'
    return app
