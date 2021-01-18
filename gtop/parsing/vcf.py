"""
VCF parsing utilities
"""

def vcf_calls_auto(n_fixed_fields):
    """
    Automaton to extract GT calls from a cvf
    """
    F_states = [f'F{i}' for i in range(1, n_fixed_fields)]
    vauto = {
        **{
            F: {
                '\t': (None, F_NEXT)
            }
            for F, F_NEXT in zip(['START'] + F_states, F_states + ['GT'])
        },
        'GT': {
            r'\.': (float('nan'), 'GT'),
            '1': (1.0, 'GT'),
            '0': (0.0, 'GT'),
            ':': (None, 'NEXT_GT'),
        },
        'NEXT_GT': {
            '\t': (None, 'GT'),
        }
    }
    return vauto
