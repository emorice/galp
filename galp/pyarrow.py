"""
PyArrow utils
"""

def to_ndarray(parray):
    return (
        parray
        .combine_chunks()
        .flatten()
        .to_numpy()
        .reshape(
            len(parray),
            parray.type.list_size)
        )
