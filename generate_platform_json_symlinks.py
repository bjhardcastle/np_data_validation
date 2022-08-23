from data_getters import lims_data_getter
from psycopg2 import connect, extras
import pathlib
import subprocess
import os
import sys

# gets the platform jsons after querying lims
def get_platform_json(cursor):
    query = '''
        SELECT p.name, es.storage_directory
        FROM ecephys_sessions es
            JOIN projects p ON p.id = es.project_id

    '''

    cursor.execute(query)
    results = cursor.fetchall()
    results_list = []

    for row in results:
        d = dict(row)
        if d['name'] is not None and d['storage_directory'] is not None: # if there is a name and storage directory
            results_list.append(d)

    #print(results_list)
    return results_list

if __name__ == '__main__':
    # database connection
    lims_connection = connect(
                dbname='lims2',
                user='limsreader',
                host='limsdb2',
                password='limsro',
                port=5432,
            )

    # set settings for connection
    lims_connection.set_session(readonly=True, autocommit=True)
    cursor = lims_connection.cursor(cursor_factory=extras.RealDictCursor)
    results_query = get_platform_json(cursor) # returns list from query
    #print(results_query)
    
    # symlink
    symlink_repo = R"\\allen\programs\mindscope\workgroups\dynamicrouting\ben\platform_json_symlinks_files"
    pathlib.Path(symlink_repo).mkdir(parents=True, exist_ok=True)
    # by default, remote-to-remote symlinks are disabled in Windows
    # enable them here:
    subprocess.run('fsutil behavior set SymlinkEvaluation R2R:1',check=True,shell=True)
    #? is this persistent?

    for result in results_query:
        #print('Checking directory', result['storage_directory'])
        directory = result['storage_directory']
        name = result['name']
        for root, _, files in os.walk(pathlib.Path('/' + directory)):
            for file in files:
                if 'platformD1.json' in file: # if it is platform d1 json
                    print(file)
                    """
                    if not os.path.exists(os.path.join(symlink_repo, result['name'])): # make folder for each of the project names
                        os.mkdir(os.path.join(symlink_repo, name))
                    """
                    symlink_filename = f'{name}.{file}'
                    symlink_path = pathlib.Path(symlink_repo, symlink_filename)
                    print('Symlink path', symlink_path)
                    print()
                    target_path = pathlib.Path(root, file)
                    try:
                        symlink_path.symlink_to(target_path)
                    except FileExistsError:
                        pass
                    
                    sys.stdout.write(f" symlink created: {symlink_filename}\r")
                    sys.stdout.flush()
    

