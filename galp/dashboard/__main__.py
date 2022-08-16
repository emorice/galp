"""
Dashboard to visualize task results
"""

import json
import argparse
import logging

from wsgiref.simple_server import make_server
from flask import Flask, render_template, abort

import galp.cli
import galp.config

app = Flask(__name__)
setup = {}

@app.route('/')
def landing():
    """
    Landing page of the app
    """
    return render_template('landing.html',
            steps=setup['steps'].all_steps
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

    steps = setup['steps']

    if b_name not in steps:
        abort(404)

    step = steps[b_name]
    task = step()

    store = setup['store']

    if task.name in store:
        obj = store.get_native(task.name)
        try:
            return json.dumps(obj, indent=4)
        except TypeError:
            return 'Object found in store but cannot be displayed'

    return 'Object not in store'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    galp.cache.add_store_argument(parser, optional=True)
    galp.cli.add_parser_arguments(parser)
    args = parser.parse_args()
    galp.cli.setup('dashboard', args.log_level)
    setup.update(
        galp.config.load_config(vars(args))
        )

    with make_server('', 5000, app) as httpd:
        logging.info("Serving HTTP on port 5000")
        httpd.serve_forever()
