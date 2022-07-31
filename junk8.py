import argparse
import pathlib

import data_validation as dv

NPEXP_ROOT = R"//allen/programs/mindscope/workgroups/np-exp"
for f in pathlib.Path(NPEXP_ROOT).iterdir():

    dv.clear_npexp(
        f,  
        generate=True,
        min_age=30, # days
        delete=False,
        )
