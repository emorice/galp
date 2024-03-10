"""
Dashboard to visualize task results
"""

import json
import logging
from importlib import import_module

from flask import Flask, render_template, abort

import galp.config
import galp.commands as cm
from galp.net.requests.types import Put
from galp._client import BrokerProtocol, store_literals
from galp.task_types import TaskSerializer
from galp.cache import CacheStack
from galp.result import Ok

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

def collect_kwargs(store, task):
    """
    Re-use client logic to parse the graph and sort out which pieces
    need to be fetched from store
    """
    tdef = task.task_def

    ## Define how to fetch missing pieces (direct read from store)
    proto = None # fwd decl
    def _schedule(command: cm.InertCommand):
        if not isinstance(command, cm.Get):
            raise NotImplementedError
        name = command.name
        if name not in proto.store:
            buf, children, _loads = store.get_serial(name)
            proto.store.put_serial(name, (buf, children))
        res = Put(*proto.store.get_serial(name))
        proto.schedule_new(
            proto.script.commands[command.key].done(res)
            )
    def _write_local(_msg):
        raise NotImplementedError

    mem_store = CacheStack(dirpath=None, serializer=TaskSerializer)
    store_literals(mem_store, [task])
    proto = BrokerProtocol(_schedule, _write_local, cpus_per_task=1, store=mem_store)

    ## Collect args from local and remote store. Since we don't pass any
    ## argument to the step, all arguments are injected, and therefore keyword arguments
    assert not tdef.args
    kwargs = {}
    for keyword, tin in tdef.kwargs.items():
        # You need to hold a reference, because advance_all won't !
        cmd = cm.rget(tin.name)
        # This is recursive through _schedule, and will only return when no more
        # commands are issued
        proto.schedule_new(
            cm.advance_all(proto.script, cm.get_leaves([cmd]))
            )
        if not isinstance(cmd.val, Ok):
            # Error handling
            raise NotImplementedError(cmd.val)
        kwargs[keyword] = cmd.val.value

    return kwargs
