"""
A simple automaton compiler using llvmlite
"""

from llvmlite import ir

from ctypes import CFUNCTYPE, c_double
from ctypes import create_string_buffer, c_char_p, c_int32

from gtop.parsing.llvm import compile_ir, create_execution_engine

def acompile(auto, endchar=b'\x00', out_t=ir.IntType(8)):
    """
    Generate the IR for the given automaton.

    Args:
        endchar: alternative to null char if you're dealing with e.g. EOL-terminated string (lines).
            This avoids making a copy just to strip the terminator.
        out_t: the llvmlite.ir base type of the output array.

        TODO: output the endchar ?
    """

    # First, the boilerplate to define a function and types
    char_t = ir.IntType(8)
    chara_t = ir.ArrayType(char_t, 0)
    charap_t = ir.PointerType(chara_t)
    outa_t = ir.ArrayType(out_t, 0)
    outap_t = ir.PointerType(outa_t)
    index_t = ir.IntType(32)
    zero_index = ir.Constant(index_t, 0)
    one_index = ir.Constant(index_t, 1)
    endchar = ir.Constant(char_t, ord(endchar))
    proto = ir.FunctionType(index_t, [charap_t, outap_t])
    module = ir.Module()
    function = ir.Function(module, proto, 'filter')
    sin, sout = function.args

    entry = function.append_basic_block()
    b = ir.IRBuilder(entry)
    end = b.append_basic_block()
    with b.goto_block(end):
        # Reemit the end char and exit
        end_out_index = b.phi(index_t)
        dst = b.gep(sout, [zero_index, end_out_index])
        # todo: make optional
        #b.store(endchar, dst)
        b.ret(end_out_index)

    if not 'START' in auto:
        raise ValueError('No start state')

    # Shared information we need:
    #  * Each state entry block
    #  * The in and out counter phi bindings
    states = {}
    for name in auto:
        # Add an entry block
        state_entry = b.append_basic_block()
        # Add two phi's for the counters
        with b.goto_block(state_entry):
            in_index = b.phi(index_t)
            out_index = b.phi(index_t)
        states[name] = (state_entry, in_index, out_index)

        # If start state, add the leading branching and bindings
        if name == 'START':
            with b.goto_block(entry):
                b.branch(state_entry)
            in_index.add_incoming(zero_index, entry)
            out_index.add_incoming(zero_index, entry)

    # Internal state logic
    for name, edges in auto.items():
        state_entry, in_index, out_index = states[name]

        # Consume a character and add the generic exit
        with b.goto_block(state_entry):
            # Get the char
            src = b.gep(sin, [zero_index, in_index])
            char = b.load(src)
            next_in_index = b.add(in_index, one_index)

            # Add the generic exit condition
            is_last = b.icmp_signed('==', char, endchar)
            with b.if_then(is_last, likely=False):
                end_out_index.add_incoming(out_index, b.block)
                b.branch(end)

            # By default, do not emit anything and loop on the state itself
            if not '.' in edges:
                edges['.'] = ('', name)

            cases = {}
            # Loop over edges and create a block for each
            for event in edges:
                cases[event] = b.append_basic_block()

            # Add the switch and default branch
            switch = b.switch(char, cases['.'])

            # Fill each cases' block
            for event, (emit, transit) in edges.items():
                if event == r'\.':
                    raw_event = '.'
                else:
                    raw_event = event
                if event != '.':
                    switch.add_case(
                        ord(raw_event),
                        cases[event])
                with b.goto_block(cases[event]):
                    # Emission
                    if emit in [None, '', b'']:
                        next_out_index = out_index
                    else:
                        if emit == r'\0':
                            emitted = [char]
                        else:
                            # todo: reimplement str support
                            #emitted = [
                            #    ir.Constant(out_t, ord(c))
                            #    for c in emit
                            #]
                            emitted = [
                                ir.Constant(out_t, emit)
                            ]
                        i = out_index
                        for item in emitted:
                            dst = b.gep(sout, [zero_index, i])
                            b.store(item, dst)
                            i = b.add(i, one_index)
                        next_out_index = i

                    # Transition
                    tr_entry, tr_in, tr_out = states[transit]
                    b.branch(tr_entry)
                    tr_in.add_incoming(next_in_index, b.block)
                    tr_out.add_incoming(next_out_index, b.block)

    return module

def a_to_function(auto, endchar=b'\x00',
    out_t=ir.IntType(8), out_ctype=c_char_p,
    buf_factor=3,
    engine=create_execution_engine()
    ):
    """
    Compiles the given automaton to native code and wraps it in a python
    function (bytes->bytes).
    """
    ir = acompile(auto, endchar, out_t)
    mod = compile_ir(engine, str(ir))
    func_ptr = engine.get_function_address('filter')
    assert func_ptr
    filt = CFUNCTYPE(c_int32, c_char_p, out_ctype)(func_ptr)

    def _wrapper(string, out_buffer=None):
        buf = create_string_buffer(string)

        if out_buffer is None:
            obuf = create_string_buffer(buf_factor*len(buf))
            filt(buf, obuf)
            return obuf.value

        else:
            return filt(buf, out_buffer)

    return _wrapper
