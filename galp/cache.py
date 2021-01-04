"""
Caching utils
"""

import os
import json
import diskcache

class CacheStack():
    """Asynchronous cache proxy"""
    def __init__(self, dirpath):
        self.dirpath = dirpath
        self.memcache = dict()
        self.diskcache = diskcache.Cache(dirpath)

    async def contains(self, name):
        # Mind the short circuit
        return (name in self.memcache) or (name in self.diskcache)

    async def get(self, name):
        try:
            return self.memcache[name]
        except KeyError:
            obj = json.loads(self.diskcache[name])
            self.memcache[name] = obj
            return obj

    async def put(self, name, obj):
        self.memcache[name] = obj
        self.diskcache[name] = json.dumps(obj).encode('ascii')

