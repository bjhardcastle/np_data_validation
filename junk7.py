from operator import ge
import time
import data_validation as dv
import pathlib
import argparse
# NPEXP_ROOT = R"//allen/programs/mindscope/workgroups/np-exp"
# for f in pathlib.Path(NPEXP_ROOT).iterdir():

    # dv.clear_npexp(f,min_age=30, # days
    #     delete=False,)

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
if __name__ == "__main__":
    # define args
    parser = argparse.ArgumentParser()
    parser.add_argument('--session_folder_str', type=str)
    args = parser.parse_args()
    
    dv.clear_npexp(args.session_folder_str,generate=True,min_age=0, delete=False)# days)