"""
Steps related to the path provider feature
"""

import galp

@galp.step
def write_file(string):
    """
    Write to a unique file
    """
    path = galp.new_path()
    with open(path, 'w', encoding='utf8') as stream:
        stream.write(string)

    with open(galp.new_path(), 'w', encoding='utf8') as stream:
        stream.write('clobber !')

    return path

@galp.step
def copy_file(path):
    """
    Transfer content to a new file
    """
    dst = galp.new_path()
    with open(dst, 'w', encoding='utf8') as dst_stream:
        with open(path, encoding='utf8') as src_stream:
            dst_stream.write(src_stream.read())
    return dst

@galp.step
def read_file(path):
    """
    Return content of an utf8 text file
    """
    with open(path, encoding='utf8') as stream:
        return stream.read()
