import os
import pathlib
import random
import subprocess
import sys

import data_validation as dv

locations = [
        # R'//allen/programs/mindscope',
        # R'//allen/programs/braintv',
        R'//allen/programs/aind',
    ]

symlink_repo = R"\\allen\programs\mindscope\workgroups\dynamicrouting\ben\settings_xml_files"

pathlib.Path(symlink_repo).mkdir(parents=True, exist_ok=True)

# by default, remote-to-remote symlinks are disabled in Windows
# enable them here:
subprocess.run('fsutil behavior set SymlinkEvaluation R2R:1',check=True,shell=True)
#? is this persistent?

def hash_path(path):
    return int((hash(pathlib.Path(path).as_posix())**2)**0.5)

for location in locations:
    print(f"checking {location}")
    
    for root, _, files in os.walk(pathlib.Path(location), followlinks=True):
            for file in files:
                if file == 'settings.xml':
                    
                    target_path = pathlib.Path(root, file)
                    file_root = dv.Session.folder(root)
                    
                    symlink_filename = f"{file_root or hash_path(target_path)}.settings.xml"
                    symlink_path = pathlib.Path(symlink_repo, symlink_filename)
                    
                    try:
                        symlink_path.symlink_to(target_path)
                    except FileExistsError:
                        pass
                    
                    sys.stdout.write(f" symlink created: {symlink_filename}\r")
                    sys.stdout.flush()
                    