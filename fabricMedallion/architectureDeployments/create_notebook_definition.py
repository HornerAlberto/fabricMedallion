def create_notebook_definition(code_cells):
    cells =  [
            {
                'cell_type': 'code',
                'source': [code_cell],
                'outputs': [],
                'metadata': {
                    'microsoft': {
                        'language': 'python',
                        'language_group': 'synapse_pyspark'
                    },
                }
            }
            for code_cell in code_cells
    ]
    return {
        "cells": cells,
        'metadata': {
            'kernel_info': {'name': 'synapse_pyspark'},
            'kernelspec': {
                'name': 'synapse_pyspark',
                'language': 'Python',
                'display_name': 'Synapse PySpark'
            },
            'language_info': {'name': 'python'},
            'microsoft': {
                'language': 'python',
                'language_group': 'synapse_pyspark',
                'ms_spell_check': {'ms_spell_check_language': 'en'}
            },
            'spark_compute': {
                'compute_id': '/trident/default'
            },
            'dependencies': {}
        },
        'nbformat': 4,
        'nbformat_minor': 5
    }