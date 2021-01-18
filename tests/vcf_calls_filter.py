"""
A simple line filter calling the compiled automaton filter for out-of-pipeline
benchmarking
"""
import sys
from gtop.parsing.vcf import vcf_calls_auto
from gtop.parsing.autocompile import a_to_function

import numpy as np
from ctypes import POINTER, c_double
from llvmlite import ir

def main():
    auto = vcf_calls_auto(9)
    buf = np.empty([2000], np.float64)
    filt = a_to_function(auto, endchar=b'\n', out_t=ir.DoubleType(),
    out_ctype=POINTER(c_double))
    for line in sys.stdin.buffer:
        filt(line, buf.ctypes.data_as(POINTER(c_double)))

if __name__ == '__main__':
    main()
