import json
import os
import pathlib
from functools import partial
from pprint import pprint

import data_validation as dv
import timeit

db = dv.ShelveDataValidationDB
# db = dv.MongoDataValidationDB

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
x = db.DVFile(path="/test2/1234568890_366122_19700101_test.txt", checksum="00000000", size=13)
db.add_file(file=x)
x = db.DVFile(path="/test2/1234568890_366122_19700101_test.txt", checksum="00000001", size=11)
db.add_file(file=x)
x = db.DVFile(path="/test/1234568890_366122_19700101_test.txt", checksum="00000000", size=11)
db.add_file(file=x)

print(db.get_matches(file=x,match=db.DVFile.Match.SELF))
