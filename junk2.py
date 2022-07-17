import json
import os
import pathlib
from functools import partial
from pprint import pprint

import data_validation as dv

db = dv.CRC32JsonDataValidationDB("incoming_npexp.json")
# x = [f for f in db.db if f.session.folder]

not_backed_up = []
local_root = pathlib.Path("//allen/programs/mindscope/workgroups/np-exp")
incoming_root = pathlib.Path("//allen/programs/braintv/production/incoming/neuralcoding")
age_threshold_days = 60 # don't remove files from sessions younger than this

with open(dv.CRC32JsonDataValidationDB.path) as f:
    dbjson= json.load(f)
    
for f in pathlib.Path(local_root).glob("*"):
    # print(f)
    #checksum
    # check if in lims incoming
    # local = db.DVFile(str(f))
    s = f.name 
    
    if len(s.split("_")) != 3:
        print(f"{s}: has no session name")
        continue
    
    if str(s) not in str(dbjson):
        print(f"{s}: not in incoming")
        continue
    
    # check age not less than threhsold
    if int(s.split("_")[2]) > 20220618:
        print(f"{s}: not old enough")
        continue
    
    # print(f)
    
    for n in f.glob("*/*.npx2"):
        i = n.relative_to(n.parent.parent) 
        
        if not (incoming_root / i).exists():
            print(f"{i}: not found")
        else:
             
            incoming = db.DVFile(str(incoming_root / i.as_posix()))
            local = db.DVFile(str(n.as_posix()))
            print(f"{incoming}: found!")
            
            incoming.generate_checksum(incoming.path)
            db.add_file(incoming)
            local.generate_checksum(local.path)
            db.add_file(local)
            
            db.save()
            
            if incoming == local:    
                
                
                name_length_diff = len(incoming.parent + incoming.name) - len(local.parent + local.name)
                pad = " "
                if name_length_diff < 0:
                    pad_incoming = pad * abs(name_length_diff)
                    pad_local = ""
                else:
                    pad_incoming = ""
                    pad_local = pad * name_length_diff
                                    
                print("-" * 80)
                print("checksum match:")
                print(
                    f"incoming : {incoming.parent}/{incoming.name}{pad_incoming} | {incoming.checksum} | {incoming.size} bytes"
                )
                print(
                    f"np-exp: {local.parent}/{local.name}{pad_local} | {local.checksum} | {local.size} bytes"
                )
                print("")
                print(
                    f"safe to delete incoming copy {incoming.name} at {pathlib.Path(incoming.path).parent.as_uri()}"
                )

                #! disable deletion for now
                # pathlib.Path(incoming.path).unlink(missing_ok=True)

                # print("deleted")
                print("-" * 80)
        # partial_matches = db.get_matches(size=local.size)
    