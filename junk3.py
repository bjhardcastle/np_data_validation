import json
import os
import pathlib
from functools import partial
from pprint import pprint

import data_validation as dv
import timeit

# db = dv.ShelveDataValidationDB
db = dv.MongoDataValidationDB

x = db.DVFile(path="/test/1234567890_366122_19700101_test.txt")
db.add_file(file=x)
x = db.DVFile(path="/test/1234567890_366122_19700101_test2.txt", checksum="00000000", size=23)
db.add_file(file=x)
x1 = db.DVFile(path="/test/1234567890_366122_19700101_test.txt", checksum="00000000", size=23)
db.add_file(file=x1)
x = db.DVFile(path="/test/1234567890_366122_19700101_test.txt", checksum="00000000", size=23)
db.add_file(file=x)
x2 = db.DVFile(path="/test/1234568890_366122_19700101_test.txt", checksum="00220000", size=23)
db.add_file(file=x2)
x = db.DVFile(path="/test/1234568890_366122_19700101_test.txt", size=11)
db.add_file(file=x)
x = db.DVFile(path="/test2/1234568890_366122_19700101_test.txt", checksum="00000001", size=11)
db.add_file(file=x)
x = db.DVFile(path="/test/1234568890_366122_19700101_test.txt", checksum="00000000", size=11)
db.add_file(file=x)
# 
# print(db.get_matches(file=x,match=db.DVFile.Match.SELF))

# dv.report(x,x)
local = R"C:\Users\ben.hardcastle\Desktop\1190258206_611166_20220708"
npexp = R"\\w10dtsm18306\neuropixels_data\1190258206_611166_20220708"
f = dv.DataValidationFolder(local)
f.db = dv.MongoDataValidationDB
f.add_folder_to_db(local, generate_large_checksums=False)
f.add_folder_to_db(npexp, generate_large_checksums=False)

f.add_backup(npexp)

f.validate_backups(verbose=False)