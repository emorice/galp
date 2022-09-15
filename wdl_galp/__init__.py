"""
Run wdl workflows programmatically from galp
"""

import os
import threading
import asyncio
import logging

from . import handlers

import WDL
import WDL.runtime

import galp

pbl = galp.StepSet()

@pbl.step
def run(uri, _galp, **kwargs):
    """
    Wraps execution of a wdl workflow in a galp task

    Targets the `workflow` given in the file, targeting a specific task is not
    yet supported.
    """

    result = {}

    workspace = _galp.new_path()

    os.mkdir(workspace)

    thread = threading.Thread(target=_run_thread_inner, args=(uri, result,
        workspace, kwargs))

    thread.start()
    thread.join()

    return result

def _run_thread_inner(uri, out, workspace, kwargs):
    """
    Does the actual WDL calls, meant to be started in its own thread to avoid
    conflicting with the parent's asyncio event loop.

    The threading logic should likely be moved into galp core at some point.
    """
    try:
        asyncio.set_event_loop(asyncio.new_event_loop())

        doc = WDL.load(uri)

        target = doc.workflow

        inputs = WDL.Env.Bindings()

        for key, py_value in kwargs.items():
            decl = target.available_inputs.resolve(key)
            wdl_value = WDL.Value.from_json(
                decl.type,
                py_value)
            inputs = inputs.bind(key, wdl_value)

        for decl_binding in target.required_inputs:
            if decl_binding.name not in inputs:
                raise TypeError(
                'No value for required input '
                f'"{decl_binding.name}: {decl_binding.value.type}" '
                f'of workflow "{target.name}"')

        cfg = WDL.runtime.config.Loader(logging.getLogger(__name__))

        _, wdl_outputs = WDL.runtime.run(
                cfg, target, inputs, run_dir=os.path.join(workspace, '.')
                )

        out.update(WDL.values_to_json(wdl_outputs, target.name))

    except:
        logging.exception('in thread:')
        raise
