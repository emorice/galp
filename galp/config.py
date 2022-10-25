"""
Utils for managing configuration.
"""

import os
import logging
from importlib import import_module

import toml

from galp.cache import CacheStack
from galp.graph import Block
from galp.serializer import Serializer

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

    setup['steps'] = load_steps(setup.get('steps') or [])

    logging.info("Storing in %s", setup['store'])
    setup['store'] = CacheStack(setup.get('store'), Serializer())

    return setup

def load_steps(plugin_names):
    """
    Attempts to import the given modules, and add any public StepSet attribute
    to the list of currently known steps
    """
    step_dir = Block()
    for name in plugin_names:
        try:
            plugin = import_module(name)
            for k, attr in vars(plugin).items():
                if isinstance(attr, Block) and not k.startswith('_'):
                    step_dir += attr
            logging.info('Loaded plug-in %s', name)
        except ModuleNotFoundError as exc:
            logging.error('No such plug-in: %s', name)
            raise ConfigError(('No such plugin', name)) from exc
        except AttributeError as exc:
            logging.error('Plug-in %s do not expose "export"', name)
            raise ConfigError(('Bad plugin', name)) from exc
    return step_dir

def add_config_argument(parser):
    """
    Fills an argparse parser
    """
    parser.add_argument('-c', '--config',
        help='path to optional TOML configuration file')
