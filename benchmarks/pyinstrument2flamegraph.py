#!/usr/bin/env python
"""
Convert pyinstrument json report to flamegraph log
"""

import sys
import json
import math
import hashlib

def color(x):
    """Rotating color"""
    theta = 2 * math.pi * x
    red = int(128 * (1 + .999 * math.cos(theta)))
    green = int(128 * (1 + .999 * math.cos(theta - 2*math.pi/3)))
    blue = int(128 * (1 + .999 * math.cos(theta + 2*math.pi/3)))
    return red, green, blue

def print_log(frame, stack=None, file=sys.stdout, palette={}):
    """
    Recursive helper to write the frames
    """
    if stack is None:
        stack = []
    item = f"{frame['function']}:{frame['file_path_short']}:{frame['line_no']}"
    x_color = int.from_bytes(
            hashlib.md5(
                frame['file_path_short'].encode('utf8')
            ).digest()[-2:]
            ) / 2**16
    palette[item] = color(x_color)
    stack.append(item)
    total_time = frame['time']
    child_time = sum(f['time'] for f in frame['children'])
    own_time = max(total_time - child_time, 0.0)
    print(';'.join(stack), f'{own_time * 1000:f}', file=file)
    for child in frame['children']:
        print_log(child, stack, file, palette)
    stack.pop()

def print_palette(palette, file=sys.stdout):
    """Write flamegraph palette file"""
    for frame, rgb in palette.items():
        print(f'{frame}->rgb({",".join(map(str, rgb))})', file=file)

def main():
    """Main"""
    palette = {}
    print_log(json.load(sys.stdin)['root_frame'], palette=palette)
    with open('palette.map', 'w', encoding='utf-8') as fd:
        print_palette(palette, fd)

main()
