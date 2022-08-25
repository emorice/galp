"""
Dashboard to visualize task results
"""

import json
import logging
from importlib import import_module

from flask import Flask, render_template, abort

import galp.config

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
        try:
            b_name = step_name.encode('ascii')
        except UnicodeEncodeError:
            abort(404)

        steps = app.galp['steps']
        store = app.galp['store']

        if b_name not in steps:
            abort(404)

        step = steps[b_name]
        task = step()


        if step.is_view:
            result = step.function()
            is_safe, rep = render_object(result)
            return render_template('step.html', safe_obj=is_safe, obj=rep)

        if task.name in store:
            obj = store.get_native(task.name)
            is_safe, rep = render_object(obj)
            return render_template('step.html', safe_obj=is_safe, obj=rep)

        return 'Neither a view nor an object in store'
    return app
