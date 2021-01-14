"""
Profiling utils
"""

import os
import re
import cProfile
import logging

from galp.config import ConfigError

class Profiler:
    """
    Contains the profiling logic.

    Args:
        config: the config sub-dict for the 'profile' section. If None,
            profiling will be turned off.
    """

    def __init__(self, config=None):
        if config is not None:
            try:
                self.dir = config['dir']
                os.mkdir(config['dir'])
            except KeyError:
                raise ConfigError('Profiler is none but no profile.dir '
                    'was specified')
            except FileExistsError:
                pass

            if 'steps' in config:
                try:
                    self.patterns = [re.compile(pat.encode('ascii')) for pat in config['steps']]
                    logging.warning('Profiler loaded patterns: %s', self.patterns)
                except re.error as e:
                    raise ConfigError(f'Invalid step pattern {e}')
            self.on = True
        else:
            self.on = False

    def wrap(self, name, step):
        """Wrap the function in the necessary call to profiler if needed"""
        if self.on and any(pat.search(step.key) for pat in self.patterns):
            logging.warning('Profiling on for %s in %s', step.key, name)
            def _wrapped(*args, **kwargs):
                nonlocal step
                result = [None]
                cProfile.runctx('result[0] = step.function(*args, **kwargs)',
                    globals(), locals(),
                    filename=os.path.join(self.dir, name.hex() + '.profile')
                    )
                return result[0]
            return _wrapped
        return step.function

