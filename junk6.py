import data_validation as dv

f = R"\\w10dtsm18306\neuropixels_data\1191375883_615047_20220713"

session_folder = dv.DataValidationFolder(f)
session_folder.add_backup(R"//allen/programs/mindscope/workgroups/np-exp/1191375883_615047_20220713")

session_folder.db = dv.MongoDataValidationDB()

session_folder.generate_large_checksums = False

session_folder.regenerate_large_checksums = False

session_folder.upper_size_limit = 1024**0

session_folder.include_subfolders = False

session_folder.validate_backups(verbose=False)
