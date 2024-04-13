import async_timeout

# Nice pattern from https://stackoverflow.com/a/55693498
def with_timeout(t=3):
    def wrapper(corofunc):
        async def run(*args, **kwargs):
            async with async_timeout.timeout(t):
                return await corofunc(*args, **kwargs)
        return run
    return wrapper
