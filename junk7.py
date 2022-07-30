import pathlib

import data_validation as dv

dv.clear_npexp(min_age=30, # days
    delete=False,)

# for p in [
#     "B:/",  
#     "A:/",
#     ]:
#     dv.clear_dir(
#         path=p,
#         include_session_subfolders=True,
#         generate_large_checksums=True,
#         regenerate_large_checksums=False,
#         upper_size_limit=1024**1 * 5,
#         min_age=0, # days
#         delete=True,
#     )

