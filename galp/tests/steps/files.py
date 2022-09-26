"""
Steps related to the path provider feature
"""

from galp import StepSet

export = StepSet()

@export
def write_file(string, _galp):
    """
    Write to a unique file
    """
    path = _galp.new_path()
    with open(path, 'w', encoding='utf8') as stream:
        stream.write(string)

    with open(_galp.new_path(), 'w', encoding='utf8') as stream:
        stream.write('clobber !')

    return path

@export
def copy_file(path, _galp):
    """
    Transfer content to a new file
    """
    dst = _galp.new_path()
    with open(dst, 'w', encoding='utf8') as dst_stream:
        with open(path, encoding='utf8') as src_stream:
            dst_stream.write(src_stream.read())
    return dst

@export
def read_file(path):
    """
    Return content of an utf8 text file
    """
    with open(path, encoding='utf8') as stream:
        return stream.read()

@export
def injectable_writer(_galp):
    """
    Write constant to file
    """
    path = _galp.new_path()
    with open(path, encoding='utf8', mode='w') as stream:
        stream.write('wizard')
    return path

@export
def injected_copier(injectable_writer, _galp):
    dst = _galp.new_path()
    with open(dst, 'w', encoding='utf8') as dst_stream:
        with open(injectable_writer, encoding='utf8') as src_stream:
            dst_stream.write(src_stream.read())
    return dst
