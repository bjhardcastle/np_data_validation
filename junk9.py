import argparse
import pathlib
import data_validation as dv 

root = R"\\10.128.54.19\sd9"
for f in pathlib.Path(root).glob('*'):
    F = dv.DataValidationFolder(f)
    F.db = dv.MongoDataValidationDB
    backups = F.add_folder_to_db()
    try:
        for b in backups:
            dv.ShelveDataValidationDB.add_file(b)
    except:
        pass


# if __name__ == "__main__":
#     print('y')
#     # define args
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--session_folder_str', type=str)
#     args = parser.parse_args()
    
#     dv.clear_npexp(args.session_folder_str)