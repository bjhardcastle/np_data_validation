

def npexp(session_folder: Union[str, DataValidationFolder]) -> str:
    ''''''
    if not isinstance(session_folder, str):
        session_folder = str(session_folder)
    npexp_root = R"//allen/programs/mindscope/workgroups/np-exp"
    try:
        return npexp_root + '/' + Session(session_folder).folder
    except:
        return None 


def lims(session_folder: Union[str, DataValidationFolder]) -> str:
    try:
        import data_getters as dg
        if not isinstance(session_folder, str):
            session_folder = str(session_folder)
        d = dg.lims_data_getter(Session(session_folder).id)
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
def clear_npexp(folder_str, generate=True, min_age=45, # days
    delete=False) -> int:
    """Look for large npx2 files - check their age, check they have a valid copy on LIMS, then delete"""
    NPEXP_ROOT = R"//allen/programs/mindscope/workgroups/np-exp/"

    if len(pathlib.Path(folder_str).parts) == 1:
        folder_str = pathlib.Path(NPEXP_ROOT, folder_str)
    
    # db_s = ShelveDataValidationDB()
    db_m = MongoDataValidationDB()
    hostname = socket.gethostname()
    if 'hpc' in hostname or (hostname.startswith('n') and len(hostname) <= 4):
        hpc = True
        #CRC32DataValidationFile.checksum_generator = mmap_direct
    else:
        hpc=False
    
    def lims(session_folder) -> str:
        try:
            import data_getters as dg
            d = dg.lims_data_getter(session_folder.split('_')[0])
            d.get_probe_data()
            # print(pathlib.Path('/' + d.data_dict['storage_directory']))
            probe_dir = d.data_dict['storage_directory'] 
            if probe_dir:
                return '/'+probe_dir 
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
        
    try:
        f = pathlib.Path(folder_str)
        session_folder = Session(str(folder_str)).folder
        

    except ValueError:
        # not an ecephys session folder
        return None
    
    # for f in pathlib.Path(NPEXP_ROOT).iterdir():
    #     try:
    #         session_folder = Session(str(f)).folder
    #     except ValueError:
    #         # not an ecephys session folder
    #         continue
        
    # check age of experiment folder
    if int(Session(str(f)).date) \
        > int((datetime.datetime.now() - datetime.timedelta(days=min_age)).strftime('%Y%m%d')) \
        :
        print(f'skipping, less than {min_age=}: {f.date}')    
        return None
    
    total_deleted_bytes = 0
    
    def check_and_delete(DVFile):
        if "np-exp" in DVFile.path:
            if delete:
                os.path.DVFile.path.unlink()
                logging.info(f'DELETED {DVFile.path}')
            
    print(f'checking {session_folder}')
    for probe_str in ['ABC','DEF']:
        probe_folder = f / f'{session_folder}_probe{probe_str}'
        
        if probe_folder.exists():
            g = probe_folder.glob('*.npx2')
            
            if g:
                files = [file for file in g]
                
                for filepath in files:
                    npexp_npx2 = CRC32DataValidationFile(str(filepath))
                    
                    # shelve_matches = db_s.get_matches(npexp_npx2)
                    matches, match_type = db_m.get_matches(npexp_npx2)
                    bad_copy = False
                    for i, v in enumerate(match_type):
                        if v in [npexp_npx2.Match.SELF.value, npexp_npx2.Match.SELF_NO_CHECKSUM.value]: 
                            # without re-checksumming, exchange with the database entry 
                            # (we recently checksummed all npx2s in npexp in one go, so this should be safe to do)
                            npexp_npx2 = matches[i]
                            break
                        

                    else: 

                        if not generate:
                            print(f'checksum needed: {npexp_npx2.path}')
                            continue
                        # no existing entry in db:
                        print(f'generating checksum: {npexp_npx2.path}')
                        start_time = time.time()
                        npexp_npx2.checksum = npexp_npx2.generate_checksum(npexp_npx2.path) 
                        print(f'..completed in {time.time() - start_time : .0f} s')
                        db_m.add_file(npexp_npx2)
                        # db_s.add_file(npexp_npx2)            
                        
                    # now we have an np-exp npx2 with a checksum, check for valid copies on lims 
                    matches, match_type = db_m.get_matches(npexp_npx2)
                    for i, v in enumerate(match_type):
                        if v in [npexp_npx2.Match.VALID_COPY_RENAMED, npexp_npx2.Match.VALID_COPY_SAME_NAME] \
                            :
                            if 'prod0' in matches[i].path and matches[i].accessible:
                                print("DELETE {}".format(matches[i].path))                        
                                report(npexp_npx2, matches[i])
                                total_deleted_bytes += npexp_npx2.size
                                check_and_delete(npexp_npx2)
                                break
                            else:
                                print(f'not accessible or not in prod0 {matches[i].path} ')
                                
                            
                        elif v >= npexp_npx2.Match.UNSYNCED_DATA.value:
                            print(f'{filepath} is {npexp_npx2.Match(v)}')
                            report(npexp_npx2, matches[i])
                            bad_copy = matches[i]
                            
                    else:
                        if bad_copy:
                            # only a bad copy exists on lims: 
                            # we need to reupload and add checksum to db
                            print(f'{bad_copy=} needs re-uploading to LIMS : {v}')
                            
                        limspath = lims(session_folder)
                        if not limspath:
                            continue
                        lims_probe_folder = pathlib.Path(limspath) / f'{session_folder}_probe{probe_str}'
                        if lims_probe_folder.exists():
                            g = lims_probe_folder.glob('*.npx2')
            
                            if g:
                                files = [file for file in g]
                
                                for filepath in files:
                                    lims_npx2 = CRC32DataValidationFile(str(filepath))
                                    if not generate:
                                        print(f'checksum needed: {lims_npx2.path}')
                                        continue
                                    # we alredy know this isn't in the db, so just checksum
                                    start_time = time.time()
                                    lims_npx2.checksum = lims_npx2.generate_checksum(lims_npx2.path) 
                                    print(f'..completed in {time.time() - start_time : .0f} s')
                                    db_m.add_file(lims_npx2)
                                    # db_s.add_file(lims_npx2)
                                    
                                    if (npexp_npx2 == lims_npx2) in [npexp_npx2.Match.VALID_COPY_RENAMED, npexp_npx2.Match.VALID_COPY_SAME_NAME]:
                                        # we already know this file is in lims and is accessible
                                        print(f'DELETE {npexp_npx2.path=}')
                                        report(npexp_npx2, lims_npx2)
                                        total_deleted_bytes += npexp_npx2.size
                                        check_and_delete(npexp_npx2)
    
    return total_deleted_bytes
                            
                        
    
    
def clear_dir(
    path: str = None,
    include_session_subfolders: bool = False,
    generate_large_checksums: bool = False,
    regenerate_large_checksums: bool = False,
    upper_size_limit=1024**3 * 5, # GB
    min_age=30, # days
    delete=False,
):
    """For a directory containing session folders, check whether valid backups exist
    then delete"""

    db_s = ShelveDataValidationDB()
    db_m = MongoDataValidationDB()



    for f in [folder for folder in pathlib.Path(path).iterdir() if folder.is_dir()]:
        if f.is_dir():
            logging.info(f'checking {f}')
            # try:
            # send to Mongodb
            session_folder = DataValidationFolder(f.as_posix())
            session_folder.db = db_m
            session_folder.include_subfolders = include_session_subfolders
            session_folder.generate_large_checksums = generate_large_checksums
            session_folder.regenerate_large_checksums = regenerate_large_checksums
            session_folder.upper_size_limit = upper_size_limit


            if not hasattr(session_folder,'session'):
                continue
            elif int(session_folder.session.date) \
                > int((datetime.datetime.now() - datetime.timedelta(days=min_age)).strftime('%Y%m%d')) \
                :
                continue
            
            files = session_folder.add_folder_to_db()

            # as a backup, send to Shelvedb
            if files:
                for file in files:
                    db_s.add_file(file)

            backup_folder = DataValidationFolder(npexp(session_folder))
            if backup_folder.accessible:
                backup_folder.db = db_m
                backup_folder.include_subfolders = include_session_subfolders
                backup_folder.generate_large_checksums = generate_large_checksums
                backup_folder.upper_size_limit = upper_size_limit

                # generate checksums for backup folder if they don't already exist
                # backups = backup_folder.add_folder_to_db()
                # # as a backup, send to Shelvedb
                # if backups:
                #     for b in backups:
                #         db_s.add_file(b)

            in_lims = False # lims(session_folder)
            if in_lims:
                backup_folder = DataValidationFolder(lims(session_folder))
                if backup_folder.accessible:
                    backup_folder.db = db_m
                    backup_folder.include_subfolders = include_session_subfolders
                    backup_folder.generate_large_checksums = generate_large_checksums
                    backup_folder.upper_size_limit = upper_size_limit

                    backups = backup_folder.add_folder_to_db()
                    # as a backup, send to Shelvedb
                    if backups:
                        for b in backups:
                            db_s.add_file(b)

            # check if there are any valid backups
            session_folder.add_backup(npexp(session_folder))
            # if in_lims:
            #     session_folder.add_backup(lims(session_folder))
            session_folder.validate_backups(verbose=True, delete=delete)

            # except Exception as e:
            #     logging.warning(f"{f.as_posix()}: {error(e)}")
            #     continue
