import os
import pathlib
import json

if __name__ == '__main__':
    # symlink
    symlink_repo = R"\\allen\programs\mindscope\workgroups\dynamicrouting\ben\platform_json_symlinks_files"

    mega_dict = {} # dictionary with key as project name and dictionaries as values

    for filename in os.listdir(symlink_repo):
        json_file = pathlib.Path(symlink_repo, filename).resolve() # get actual json file

        project_name = filename[0:filename.index('.')]
        if project_name not in mega_dict:
            mega_dict[project_name] = {}

        with open(json_file) as f:
            json_dict = json.load(f) # load json into dictionary
            if 'files' in json_dict: 
                files = json_dict['files'] # get files 

                for name in files:
                    d = mega_dict[project_name] # grab dictionary
                    if name not in d:
                        d[name] = 1 # first time seeing this
                    else:
                        d[name] += 1 # add one


    print(mega_dict)

