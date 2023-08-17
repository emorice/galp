"""
Profiling utils
"""

import os
import re
import cProfile
import logging

from galp.config import ConfigError
from galp.task_types import TaskName
from galp.graph import Step

# pylint: disable=too-few-public-methods
class Profiler:
    """
    Contains the profiling logic.

    Args:
        config: the config sub-dict for the 'profile' section. If None,
            profiling will be turned off.
    """

    def __init__(self, config=None) -> None:
        if config is not None:
            try:
                self.dir = config['dir']
            except KeyError as exc:
                raise ConfigError('Profiler is none but no profile.dir '
                    'was specified') from exc
            os.makedirs(config['dir'], exist_ok=True)

            if 'steps' in config:
                try:
                    self.patterns = [re.compile(pat) for pat in config['steps']]
                    logging.info('Profiler loaded patterns: %s', self.patterns)
                except re.error as exc:
                    raise ConfigError(f'Invalid step pattern {exc}') from exc
            self.is_on = True
        else:
            self.is_on = False

    def wrap(self, name: TaskName, step: Step):
        """Wrap the function in the necessary call to profiler if needed"""
        if self.is_on and any(pat.search(step.key) for pat in self.patterns):
            logging.warning('Profiling on for %s in %s', step.key, name)
            # pylint: disable=unused-argument # locals() magic
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
