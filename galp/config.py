"""
Utils for managing configuration.
"""

import os
import logging

import toml

from galp.store import Store
from galp.task_types import TaskSerializer

class ConfigError(Exception):
    """
    The given configuration is invalid
    """

def load_config(config=None, mandatory=None):
    """
    Parse the configuration from the given input and config file, load step sets
    and create store object. Important: this function can load arbitrary modules
    from the config in the current interpreter.
    """
    config = config or {}
    setup = dict(config)

    configfile = config.get('config')
    default_configfile = 'galp.cfg'

    if not configfile and os.path.exists(default_configfile):
        configfile = default_configfile

    if configfile:
        logging.info("Loading config file %s", configfile)
        # TOML is utf-8 by spec
        with open(configfile, encoding='utf-8') as fstream:
            file_config = toml.load(fstream)
        for k, val in file_config.items():
            if setup.get(k):
                setup[k].extend(val)
            else:
                setup[k] = val

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

def add_config_argument(parser):
    """
    Fills an argparse parser
    """
    parser.add_argument('-c', '--config',
        help='path to optional TOML configuration file')
