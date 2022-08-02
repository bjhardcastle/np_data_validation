import argparse
import datetime
import os
import pathlib
import re
import time
from operator import ge
from typing import Union

import data_validation as dv

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
INCOMING_ROOT = R"//allen/programs/braintv/production/incoming/neuralcoding/"

def main():
    # define args
    parser = argparse.ArgumentParser()
    parser.add_argument('--session_folder_str', type=str)
    args = parser.parse_args()
    
    if args.session_folder_str:
        dv.clear_npexp(args.session_folder_str,generate=True,min_age=0, delete=False)# days)
    else:
        npx2_on_lims = []
        npx2_not_on_lims = []
        npx2_for_deletion = []
        
        # clear out npx2 ----------------------------------------------------------------------- #
        SKIP = False
        for incoming in pathlib.Path(INCOMING_ROOT).rglob('*.npx2'):
            if SKIP:
                continue
            # dv.clear_npexp(f,generate=True,min_age=0, delete=False)# days)
            try:
                session_folder = dv.Session(str(incoming)).folder
            except:
                # no session folder available: skip
                continue
            
            lims = lims_dir(incoming)
            lims_npx2_path = pathlib.Path(lims) / incoming.relative_to(incoming.parent.parent)
            
            incoming_size = os.stat(str(incoming)).st_size
            
            if lims_npx2_path.exists():
                # check filesize matches between incoming / lims
                lims_size = os.stat(str(lims_npx2_path)).st_size
                npx2_on_lims.append(lims_size)
                age = age_days(session_folder)
                if lims_size == incoming_size:
                    print(f'{incoming.name} is on {lims_npx2_path.absolute()=}')
                    print(f'{session_folder}: {age=} days')
                    npx2_for_deletion.append(incoming_size)
                    # incoming.unlink()
            else:
                print(f'{session_folder} NOT on {str(incoming)}')
                npx2_not_on_lims.append(incoming_size)
                
        print(f'{len(npx2_for_deletion) = } {sum(npx2_for_deletion)/1024**4: .1f} Tb')
        print(f'{len(npx2_on_lims) = } {sum(npx2_on_lims)/1024**4: .1f} Tb')
        print(f'{len(npx2_not_on_lims) = } {sum(npx2_not_on_lims)/1024**4: .1f} Tb')
        
        # clear out dat ----------------------------------------------------------------------- #
        SKIP = True
        for incoming in pathlib.Path(INCOMING_ROOT).rglob('*continuous.dat'):
            if SKIP:
                continue 
            
            # dv.clear_npexp(f,generate=True,min_age=0, delete=False)# days)
            try:
                session_folder = dv.Session(str(incoming)).folder
            except:
                # no session folder available: skip
                continue
            
            lims = lims_sorted(incoming)
            lims_npx2_path = pathlib.Path(lims) / incoming.relative_to(incoming.parent.parent)
            
            incoming_size = os.stat(str(incoming)).st_size
            
            if lims_npx2_path.exists():
                # check filesize matches between incoming / lims
                lims_size = os.stat(str(lims_npx2_path)).st_size
                npx2_on_lims.append(lims_size)
                age = age_days(session_folder)
                if lims_size == incoming_size:
                    print(f'{incoming.name} is on {lims_npx2_path.absolute()=}')
                    print(f'{session_folder}: {age=} days')
                    npx2_for_deletion.append(incoming_size)
                    # incoming.unlink()
            else:
                # print(f'{session_folder} NOT on {lims=}')
                npx2_not_on_lims.append(incoming_size)
                
        print(f'{len(npx2_for_deletion) = } {sum(npx2_for_deletion)/1024**4: .1f} Tb')
        print(f'{len(npx2_on_lims) = } {sum(npx2_on_lims)/1024**4: .1f} Tb')
        print(f'{len(npx2_not_on_lims) = } {sum(npx2_not_on_lims)/1024**4: .1f} Tb')
        
def age_days(session_folder: str) -> int:
    yymmdd = dv.Session(str(session_folder)).date
    return (datetime.datetime.now() - datetime.datetime(int(yymmdd[0:4]), int(yymmdd[4:6]), int(yymmdd[6:8]))).days
            


def lims_dir(session_folder: Union[str, dv.DataValidationFolder]) -> str:
    try:
        import data_getters as dg
        if not isinstance(session_folder, str):
            session_folder = str(session_folder)
        d = dg.lims_data_getter(dv.Session(session_folder).id)
        WKF_QRY =   '''
                    SELECT es.storage_directory
                    FROM ecephys_sessions es
                    WHERE es.id = {}
                    '''
        d.cursor.execute(WKF_QRY.format(d.lims_id))
        exp_data = d.cursor.fetchall()
        if exp_data and exp_data[0]['storage_directory']:
            return str('/'+exp_data[0]['storage_directory'])
        else:
            return None
    except:
        return None
    
    
# def lims_sorted(path: Union[str, dv.DataValidationFolder]) -> pathlib.Path:
#     if not isinstance(path, str):
#         path = str(path)
#     try:
#         session_folder = dv.Session(path).folder 
           
#         lims_root = lims_dir(session_folder)
#         probe = re.match('probe[A-Z]', str(path)).group(1)

#         if exp_data and exp_data[0]['storage_directory']:
#             return str('/'+exp_data[0]['storage_directory'])
#         else:
#             return None
#     except:
#         return None
    
    
if __name__ == "__main__":
    main()
