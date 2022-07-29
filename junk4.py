import pathlib

import data_validation as dv

# h = dv.ShelveDataValidationDB()
# g = dv.CRC32JsonDataValidationDB()
# f = dv.MongoDataValidationDB()
# sync DBs ----------------------------------------------------------------------------- #

# g.load(R"\\allen\ai\homedirs\ben.hardcastle\lims_npx2_list_hashed copy.json")

# for e in g.db:
#     print(e.checksum)
#     f.add_file(e)
#     h.add_file(e)

# add npexp ---------------------------------------------------------------------------- #

# npexp = R"//allen/programs/mindscope/workgroups/np-exp"

# for p in [folder for folder in pathlib.Path(npexp).iterdir() if folder.is_dir()]:
#     if p.is_dir():

#         try:
#             F = dv.DataValidationFolder(p.as_posix())
#             F.db = f
#             files = F.add_folder_to_db(generate_large_checksums=False)
#             F.db = h

#             for file in files:
#                 F.db.add_file(file)

#         except Exception as e:
#             print(e)
#             print(f"{p}: failed")
#             continue
import timing

for p in [
    "B:/",  
    "A:/",
    ]:
    dv.clear_dir(
        path=p,
        include_session_subfolders=True,
        generate_large_checksums=True,
        regenerate_large_checksums=False,
        upper_size_limit=1024**1 * 5,
        min_age=0, # days
        delete=True,
    )
