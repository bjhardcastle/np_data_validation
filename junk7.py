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
#     )\
NPEXP_ROOT = R"//10.128.50.43/sd6.3"

def main():
    # define args
    parser = argparse.ArgumentParser()
    parser.add_argument('--session_folder_str', type=str)
    args = parser.parse_args()
    
    if args.session_folder_str:
        dv.clear_npexp(args.session_folder_str,generate=True,min_age=0, delete=False)# days)
    else:
        total_deleted_bytes = 0
        for f in pathlib.Path(NPEXP_ROOT).glob('*'):
            total_deleted_bytes += dv.clear_npexp(f,generate=True,min_age=0, delete=False) or 0
            print(f'{total_deleted_bytes/2**40 : .1f} Tb deleted')      

if __name__ == "__main__":
    main()