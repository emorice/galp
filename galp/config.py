"""
Utils for managing configuration.
"""

import logging

from galp.store import Store
from galp.task_types import TaskSerializer

class ConfigError(Exception):
    """
    The given configuration is invalid
    """

def load_config(config=None, mandatory=None):
    """
    Parse the configuration from the given input and create store object.
    """
    config = config or {}
    setup = dict(config)

    # Validate config
    mandatory = mandatory or []
    mandatory = set(mandatory)
    mandatory.add('store')
    for key in mandatory:
        if not setup.get(key):
            raise ConfigError(f'Missing "{key}"')

    logging.info("Storing in %s", setup['store'])
    setup['store'] = Store(setup.get('store'), TaskSerializer)

    return setup
