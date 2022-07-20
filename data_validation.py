""" data integrity database class stuff 


some example usage:

x = DataValidationFileCRC32(
    path=
    R"\\allen\programs\mindscope\workgroups\np-exp\1190290940_611166_20220708\1190258206_611166_20220708_surface-image1-left.png"
)
print(f"checksum auto-generated for small files: {x.checksum=})

y = DataValidationFileCRC32(checksum=x.checksum, size=x.size, path="/dir/1190290940_611166_20220708_foo.png")

# DataValidationFile objects evaulate to True if they have the same checksum and size
print(x == y)

db = data_validation.CRC32JsonDataValidationDB()
db.add_file(x)
db.save()
print(db.path)


# applying to a folder
local = R"C:\Users\ben.hardcastle\Desktop\1190258206_611166_20220708"
npexp = R"\\w10dtsm18306\neuropixels_data\1190258206_611166_20220708"
f = dv.DataValidationFolder(local)
f.db = dv.MongoDataValidationDB
f.add_folder_to_db(local, generate_large_checksums=False)
f.add_folder_to_db(npexp, generate_large_checksums=False)

f.add_backup(npexp)

f.validate_backups(verbose=True)


# to see large checksum performance (~400GB file)
db.DVFile.generate_checksum("//allen/programs/mindscope/production/incoming/recording_slot3_2.npx2")
"""

import abc
import dataclasses
import enum
import json
import os
import pathlib
import pdb
import pprint
import re
import shelve
import sys
import tempfile
import zlib
from multiprocessing.sharedctypes import Value
from sqlite3 import dbapi2
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union
from warnings import WarningMessage

try:
    import pymongo
except ImportError:
    print("pymongo not installed")


def progressbar(it,
                prefix="",
                size=40,
                file=sys.stdout,
                units: str = None,
                unit_scaler: int = None,
                display: bool = True):
    # from https://stackoverflow.com/a/34482761
    count = len(it)

    def show(j):
        if display:
            x = int(size * j / (count if count != 0 else 1))
            file.write("%s[%s%s] %i/%i %s\r" % (prefix, "#" * x, "." *
                                                (size-x), j * unit_scaler, count * unit_scaler, units or ""))
            file.flush()

    for i, item in enumerate(it):
        yield item
        show(i + 1)
    if display:
        file.write("\n")
        file.flush()


def chunk_crc32(fpath: Union[str, pathlib.Path]) -> str:
    """ generate crc32 with for loop to read large files in chunks """

    chunk_size = 65536 # bytes

    # don't show progress bar for small files
    display = True if os.stat(fpath).st_size > 10 * chunk_size else False

    crc = 0
    with open(str(fpath), 'rb', chunk_size) as ins:
        for _ in progressbar(range(int((os.stat(fpath).st_size / chunk_size)) + 1),
                             prefix="generating crc32 checksum ",
                             units="B",
                             unit_scaler=chunk_size,
                             display=display):
            crc = zlib.crc32(ins.read(chunk_size), crc)

    return '%08X' % (crc & 0xFFFFFFFF)


def test_crc32_function(func):
    temp = os.path.join(tempfile.gettempdir(), 'checksum_test')
    with open(os.path.join(temp), 'wb') as f:
        f.write(b'foo')
    assert func(temp) == "8C736521", "checksum function incorrect"


def valid_crc32_checksum(value: str) -> bool:
    """ validate crc32 checksum """
    if isinstance(value, str) and len(value) == 8 \
        and all(c in '0123456789ABCDEF' for c in value.upper()):
        return True
    return False


class Session:
    """Get session information from any string: filename, path, or foldername"""

    # use staticmethods with any path/string, without instantiating the class:
    #
    #  Session.mouse(
    #  "c:/1234566789_611166_20220708_surface-image1-left.png"
    #   )
    #  >>> "611166"
    #
    # or instantiate the class and reuse the same session:
    #   session = Session(
    #  "c:/1234566789_611166_20220708_surface-image1-left.png"
    #   )
    #   session.id
    #   >>> "1234566789"
    id = None
    mouse = None
    date = None

    def __init__(self, path: str):
        if not isinstance(path, str):
            raise TypeError(f"{self.__class__} path must be a string")

        self.folder = self.__class__.folder(path)
        # TODO maybe not do this - could be set to class without realizing - just assign for instances

        # extract the constituent parts of the session folder
        self.id = self.folder.split('_')[0]
        self.mouse = self.folder.split('_')[1]
        self.date = self.folder.split('_')[2]

    @classmethod
    def folder(cls, path) -> Union[str, None]:
        """Extract [10-digit session ID]_[6-digit mouse ID]_[6-digit date
        str] from a file or folder path"""

        # identify a session based on
        # [10-digit session ID]_[6-digit mouseID]_[6-digit date str]
        session_reg_exp = "[0-9]{0,10}_[0-9]{0,6}_[0-9]{0,8}"

        session_folders = re.findall(session_reg_exp, path)
        if session_folders:
            if not all(s == session_folders for s in session_folders):
                UserWarning(f"Mismatch between session folder strings - file may be in the wrong filder: {path}")
            return session_folders[0]
        else:
            return None


class SessionFile:
    """ Represents a single file belonging to a neuropixels ecephys session """

    session = None

    def __init__(self, path: str):
        """ from the complete file path we can extract some information upon
        initialization """

        if not isinstance(path, (str, pathlib.Path)):
            raise TypeError(f"{self.__class__}: path must be a str pointing to a file: {type(path)=}")
        if isinstance(path, pathlib.Path):
            path = str(path)

        self.accessible = os.path.exists(path)
        # ensure the path is a file, not directory
        # if the file doesn't exist, we have to assume based on lack of file extension
        if not self.accessible:
            if os.path.splitext(path)[1] == '':
                is_file = False
            else:
                is_file = True
        else:
            is_file = os.path.isfile(path)

        if not is_file:
            raise ValueError(f"{self.__class__}: path must point to a file {path=}")
        else:
            self.path = path

        self.name = os.path.basename(self.path)

        # get the name of the folder the file lives in (which may be the same as root_path below)
        self.parent = pathlib.Path(os.path.dirname(self.path)).parts[-1]

        # extract the session ID from anywhere in the path
        self.session = Session(self.path)
        if self.session:

            # we expect the session_folder string to first appear in the path as
            # a child of some 'repository' of session folders, or there rare
            # loose individual files - split the path at the first
            # session_folder match and call that folder the root
            parts = pathlib.Path(self.path).parts
            while parts:
                if self.session.folder in parts[0]:
                    break
                parts = parts[1:]
            else:
                raise ValueError(f"{self.__class__}: session_folder not found in path {self.path=}")
            self.root_path = self.path.split(str(parts[0]))[0]

            # if the repository contains session folders, it should contain the
            # following:
            session_folder_path = os.path.join(self.root_path, self.session.folder)

            # but this may not exist: we could have a file sitting in a folder
            # with assorted files from multiple sessions (e.g. LIMS incoming),
            # or a folder which has the session_folder pattern with extra info
            # appended, eg. _probeABC:
            if os.path.exists(session_folder_path):
                self.session_folder_path = session_folder_path
            else:
                self.session_folder_path = None

            # wherever the file is, get its path relative to the parent of a
            # hypothetical session folder ie. session_id/.../filename.ext :
            session_relative_path = pathlib.Path(self.path).relative_to(self.root_path)
            if session_relative_path.parts[0] != self.session.folder:
                self.relative_path = os.path.join(self.session.folder, str(session_relative_path))
            else:
                self.relative_path = str(session_relative_path)

        else:
            raise ValueError(f"{self.__class__}: path does not contain a session ID {path=}")

    def __lt__(self, other):
        if self.session.id == other.session.id:
            return self.relative_path < other.relative_path
        return self.session.id < other.session.id


class DataValidationFile(abc.ABC):
    """ Represents a file to be validated
        
        Not to be used directly, but rather subclassed.
        Can be subclassed easily to change the checksum database/alogrithm
        
        Call <superclass>.__init__(path, checksum, size) in subclass __init__  
        
    """
    # DB: DataValidationDB = NotImplemented

    checksum_threshold: int = 50 * 1024**2
    # filesizes below this will have checksums auto-generated on init

    checksum_name: str = None
    # used to identify the checksum type in the databse, e.g. a key in a dict

    checksum_generator: Callable[[str], str] = NotImplementedError()
    # implementation of algorithm for generating checksums, accepts a path and
    # returns a checksum string

    checksum_test: Callable[[Callable], None] = NotImplementedError()
    # a function that confirms checksum_generator is working as expected,
    # accept a function, return nothing but raise exception if test fails

    checksum_validate: Callable[[str], bool] = NotImplementedError()

    # a function that accepts a string and confirms it conforms to the checksum
    # format, return True or False

    # @abc.abstractmethod
    def __init__(self, path: str = None, checksum: str = None, size: int = None):
        """ setup depending on the inputs """

        if not (path or checksum):
            raise ValueError(f"{self.__class__}: either path or checksum must be set")

        if path and not isinstance(path, str):
            raise TypeError(f"{self.__class__}: path must be a str pointing to a file: {type(path)=}")

        self.accessible = os.path.exists(path)
        # ensure the path is a file, not directory
        # if the file doesn't exist, we have to assume based on lack of file extension
        if not self.accessible:
            if os.path.splitext(path)[1] == '':
                is_file = False
            else:
                is_file = True
        else:
            is_file = os.path.isfile(path)

        if not is_file:
            raise ValueError(f"{self.__class__}: path must point to a file {path=}")
        else:
            self.path = pathlib.Path(path).as_posix()

        self.name = os.path.basename(self.path)

        if path and not size and self.accessible: # TODO replace exists check, race condition
            self.size = os.path.getsize(path)
        elif size and isinstance(size, int):
            self.size = size
        elif size and not isinstance(size, int):
            raise ValueError(f"{self.__class__}: size must be an integer {size}")
        else:
            self.size = None

        if checksum:
            self.checksum = checksum

        if not checksum \
            and self.path and os.path.exists(self.path) \
            and self.size and self.size < self.checksum_threshold \
            :
            self.checksum = self.__class__.generate_checksum(self.path)

    @classmethod
    def generate_checksum(cls, path: str = None) -> str:
        cls.checksum_test(cls.checksum_generator)
        return cls.checksum_generator(path)

    @property
    def checksum(self) -> str:
        if not hasattr(self, '_checksum'):
            return None
        return self._checksum

    @checksum.setter
    def checksum(self, value: str):
        if self.__class__.checksum_validate(value):
            # print(f"setting {self.checksum_name} checksum: {value}")
            self._checksum = value
        else:
            raise ValueError(f"{self.__class__}: trying to set an invalid {self.checksum_name} checksum")

    def __repr__(self):
        return f"(path='{self.path or ''}', checksum='{self.checksum or ''}', size={self.size or ''})"

    def __lt__(self, other):
        if self.name and other.name:
            if self.name == other.name:
                return self.checksum < other.checksum \
                    or self.size < other.size
            else:
                return self.name < other.name
        else:
            return self.checksum < other.checksum \
                or self.size < other.size

    @enum.unique
    class Match(enum.IntFlag):
        SELF = 5
        SELF_NO_CHECKSUM = 55
        OTHER_NO_CHECKSUM = 555
        VALID_COPY_SAME_NAME = 10
        VALID_COPY_RENAMED = 11
        UNSYNCED_DATA = 20
        UNSYNCED_CHECKSUM = 21
        CORRUPT_DATA = 22
        CHECKSUM_COLLISION = 30
        UNRELATED = 0

    def __eq__(self, other):
        # print("Testing checksum equality and filesize equality:")
        # if (self.checksum == other.checksum and self.size == other.size) \
        # or (self.checksum == other.checksum and self.size == other.size and self.path == other.path):
        if (self.checksum == other.checksum) \
            and (self.size == other.size) \
            and (self.path == other.path) \
            : # self
            return self.__class__.Match.SELF.value

        elif (self.size == other.size) \
            and (self.path == other.path) \
            and (not self.checksum) \
            and (other.checksum) \
            : # self without checksum confirmation (self missing)
            return self.__class__.Match.SELF_NO_CHECKSUM.value

        elif (self.size == other.size) \
            and (self.path == other.path) \
            and (self.checksum) \
            and not (other.checksum) \
            : # self without checksum confirmation (other missing)
            return self.__class__.Match.OTHER_NO_CHECKSUM.value

        elif (self.checksum == other.checksum) \
            and (self.size == other.size) \
            and (self.name == other.name) \
            and (self.path != other.path) \
            : # valid copy, not self
            return self.__class__.Match.VALID_COPY_SAME_NAME.value

        elif (self.checksum == other.checksum) \
            and (self.size == other.size) \
            and (self.name != other.name) \
            and (self.path != other.path) \
            : # valid copy, different name
            return self.__class__.Match.VALID_COPY_RENAMED.value

        elif (self.name == other.name) \
            and (self.path != other.path) \
            : # invalid copy ( multiple categories)

            if (self.size != other.size) \
                and (self.checksum != other.checksum) \
                : # out-of-sync copy or incorrect data named as copy
                return self.__class__.Match.UNSYNCED_DATA.value

            if (self.size != other.size) \
                and (self.checksum == other.checksum) \
                : # out-of-sync copy or incorrect data named as copy
                # plus checksum which needs updating
                # (different size with same checksum isn't possible)
                return self.__class__.Match.UNSYNCED_CHECKSUM.value

            if (self.size == other.size) \
                and (self.checksum != other.checksum) \
                : # possible data corruption, or checksum needs updating
                return self.__class__.Match.CORRUPT_DATA.value

        elif (self.checksum == other.checksum) \
            and (self.size != other.size) \
            and (self.name != other.name) \
            : # possible checksum collision
            return self.__class__.Match.CHECKSUM_COLLISION.value

        else:
            # apparently unrelated files (different name && checksum && size)
            return self.__class__.Match.UNRELATED.value


class CRC32DataValidationFile(DataValidationFile, SessionFile):

    # DB: DataValidationDB = CRC32JsonDataValidationDB()

    checksum_name: str = "crc32"
    # used to identify the checksum type in the databse, e.g. a key in a dict

    checksum_generator: Callable[[str], str] = chunk_crc32
    # implementation of algorithm for generating checksums, accept a path and return a checksum

    checksum_test: Callable[[Callable], None] = test_crc32_function
    # a test Callable that confirms checksum_generator is working as expected, accept a function, return nothing (raise exception if test fails)

    checksum_validate: Callable[[str], bool] = valid_crc32_checksum

    # a function that accepts a string and validates it conforms to the checksum format, returning boolean

    def __init__(self, path: str = None, checksum: str = None, size: int = None):
        # if the path doesn't contain a session_id, this will raise an error:
        SessionFile.__init__(self, path)
        DataValidationFile.__init__(self, path=path, checksum=checksum, size=size)
        # if not hasattr(self, "accessible"):
        #     self.accessible = os.path.exists(self.path)


class DataValidationDB(abc.ABC):
    """ Represents a database of files with validation metadata

    serves as a template for interacting with a database of filepaths,
    filesizes, and filehashes, for validating data integrity
    
    not to be used directly, but subclassed: make a new subclass that implements
    each of the "abstract" methods specified in this class
    
    as long as the subclass methods accept the same inputs and output the
    expected results, a new database subclass can slot in to replace an old one
    in some other code without needing to make any other changes to that code
    
    Some design notes:
    
    - hash + filesize uniquely identify data, regardless of path 
    
    - the database holds previously-generated checksum hashes for
    large files (because they can take a long time to generate), plus their
    filesize at the time of checksum generation
    
    - small text-like files can have checksums generated on the fly
    so don't need to live in the database (but they could)
    
    for a given data file input we want to identify in the database:
        - self:
            - size[0] == size[1]
            - hash[0] == hash[1]
            - path[0] == path[1]
    
        - valid backups:
            - size[0] == size[1]
            - hash[0] == hash[1]
            - path[0] != path[1]
                
            - valid backups, with filename mismatch:
                - filename[0] != filename[1]                
            
        - invalid backups:
            - path[0] != path[1] 
            - filename[0] == filename[1]
            
            - invalid backups, corruption likely:
                - size[0] == size[1]
                - hash[0] != hash[1]
            
            - invalid backups, out-of-sync or incomplete transfer:       
                - size[0] != size[1]
                - hash[0] != hash[1]
                
        - other, assumed unrelated:
            - size[0] != size[1]
            - hash[0] != hash[1]
            - filename[0] != filename[1]
                
    """
    #* methods:
    #* add_file(file: DataValidationFile)
    #* get_matches(file: DataValidationFile) -> List[DataValidationFile]
    #   the file above can be compared with entries in the returned list for further details

    DVFile: DataValidationFile = NotImplemented

    #? both of these could be staticmethods

    @abc.abstractmethod
    def add_file(self, file: DataValidationFile):
        """ add a file to the database """
        raise NotImplementedError

    @abc.abstractmethod
    def get_matches(self,
                    file: DataValidationFile,
                    path: str = None,
                    size: int = None,
                    checksum: str = None,
                    match: int = None) -> List[DataValidationFile]: #, Optional[List[int]]:
        """search database for entries that match any of the given arguments 
        """
        raise NotImplementedError


class ShelveDataValidationDB(DataValidationDB):
    """
    A database that stores data in a shelve database
    """
    DVFile: DataValidationFile = CRC32DataValidationFile
    db = "shelve_by_session_id"

    @classmethod
    def add_file(
        cls,
        file: DataValidationFile = None,
        path: str = None,
        size: int = None,
        checksum: str = None,
    ):
        """ add an entry to the database """
        if not file:
            file = cls.DVFile(path=path, size=size, checksum=checksum)

        key = file.session.id

        with shelve.open(cls.db, writeback=True) as db:
            if key in db and \
                [x for x in db[key] if (x == file) == cls.DVFile.Match.SELF] \
                :
                pass
            elif key in db:
                db[key].append(file)
            else:
                db[key] = [file]

    # @classmethod
    # def save(cls):
    #     self.db.sync()

    @classmethod
    def get_matches(cls,
                    file: DataValidationFile = None,
                    path: str = None,
                    size: int = None,
                    checksum: str = None,
                    match: int = None) -> List[DataValidationFile]: #, Optional[List[int]]:
        """search database for entries that match any of the given arguments 
        """
        if not file:
            file = cls.DVFile(path=path, size=size, checksum=checksum)

        key = file.session.id

        with shelve.open(cls.db, writeback=False) as db:
            if key in db:
                matches = db[key]

        if match and isinstance(match, int) and \
            (match in [x.value for x in cls.DVFile.Match]
             or match in [x for x in cls.DVFile.Match]):
            return [o for o in matches if (o == file) == match], [(o == file) for o in matches]
        else:
            return matches, [(o == file) for o in matches]

    def __del__(self):
        self.db.close()


class MongoDataValidationDB(DataValidationDB):
    """
    A database that stores data in a shelve database
    """
    DVFile: DataValidationFile = CRC32DataValidationFile
    db_address = "mongodb://10.128.50.77:27017/"
    db = pymongo.MongoClient(db_address).prod.snapshots

    @classmethod
    def add_file(
        cls,
        file: DataValidationFile = None,
        path: str = None,
        size: int = None,
        checksum: str = None,
    ):
        """ add an entry to the database """
        if not file:
            file = cls.DVFile(path=path, size=size, checksum=checksum)

        cls.db.insert_one({
            "session_id": file.session.id,
            "path": file.path,
            "checksum": file.checksum,
            "size": file.size,
            "type": file.checksum_name,
        })

    @classmethod
    def get_matches(cls,
                    file: DataValidationFile = None,
                    path: str = None,
                    size: int = None,
                    checksum: str = None,
                    match: Union[int, Type[enum.IntEnum]] = None) -> List[DataValidationFile]: #, Optional[List[int]]:
        """search database for entries that match any of the given arguments 
        """
        if not file:
            file = cls.DVFile(path=path, size=size, checksum=checksum)

        # with cls.db as db:
        entries = list(cls.db.find({
            "session_id": file.session.id,
        }))                                  # .distinct("_id")

        matches = [
            cls.DVFile(
                path=entry['path'],
                checksum=entry['checksum'],
                size=entry['size'],
            ) for entry in entries
        ]

        if match and isinstance(match, int) and \
            (match in [x.value for x in cls.DVFile.Match]
            or match in [x for x in cls.DVFile.Match]):

            return [o for o in matches if (o == file) == match > 0], \
                [(o == file) for o in matches if (o == file) == match > 0]

        else:
            return [o for o in matches if (o == file) > 0], \
                [(o == file) for o in matches if (o == file) > 0]

    def __del__(self):
        self.db.close()


class CRC32JsonDataValidationDB(DataValidationDB):
    """ Represents a database of files with validation metadata in JSON format
    
    This is a subclass of DataValidationDB that stores the data in a JSON
    file.
    
    The JSON file is a dictionary of dictionaries, with the following keys:
        - dir_name/filename.extension: 
                - windows: the path to the file with \\
                - posix: the path to the file with /
                - size: the size of the file in bytes
                - crc32: the checksum of the file
    
    """

    DVFile = CRC32DataValidationFile

    path = '//allen/ai/homedirs/ben.hardcastle/crc32_data_validation_db.json'

    db: List[DataValidationFile] = []

    def __init__(self, path: str = None):
        if path:
            self.path = path
        self.load(self.path)

    def load(self, path: str = None):
        """ load the database from disk """

        # persistence in notebooks causes db to grow every execution
        if self.db:
            self.db = []

        if not path:
            path = self.path

        if os.path.basename(path) == 'checksums.sums' \
            or os.path.splitext(path)[-1] == '.sums':
            # this is a text file exported by openhashtab

            # first line might be a header (optional)
            """
            crc32#implant_info.json#1970.01.01@00.00:00
            C8D91EAB *implant_info.json
            crc32#check_crc32_db.py#1970.01.01@00.00:00
            427608DB *check_crc32_db.py
            ...
            """
            root = pathlib.Path(path).parent.as_posix()

            with open(path, 'r') as f:
                lines = f.readlines()

            if not ("@" or "1970") in lines[0]:
                # this is probably a header line, skip it
                lines = lines[1:]

            for idx in range(0, len(lines), 2):
                line0 = lines[idx].rstrip()
                line1 = lines[idx + 1].rstrip()

                if "crc32" in line0:
                    crc32, *args = line1.split(' ')
                    filename = ' '.join(args)

                    if filename[0] == "*":
                        filename = filename[1:]
                    path = '/'.join([root, filename])

                    try:
                        file = self.DVFile(path=path, checksum=crc32)
                        self.add_file(file=file)
                    except ValueError as e:
                        print('skipping file with no session_id')
                        # return

        else:
            # this is one of my simple flat json databases - exact format
            # changed frequently, try to account for all possibilities

            if os.path.exists(path):

                with open(path, 'r') as f:
                    items = json.load(f)

                for item in items:
                    keys = items[item].keys()

                    if 'posix' in keys or 'linux' in keys:
                        path = items[item]['posix' or 'linux']
                    elif 'windows' in keys:
                        path = items[item]['windows']
                    else:
                        path = None

                    checksum = items[item][self.DVFile.checksum_name] \
                        if self.DVFile.checksum_name in keys else None
                    size = items[item]['size'] if 'size' in keys else None

                    try:
                        file = self.DVFile(path=path, checksum=checksum, size=size)
                        if ".npx2" or ".dat" in path:
                            self.add_file(file=file, checksum=checksum, size=size) # takes too long to check sizes here
                        else:
                            self.add_file(file=file)
                    except ValueError as e:
                        print('skipping file with no session_id')
                                                                                   # return

    def save(self):
        """ save the database to disk as json file """

        with open(self.path, 'r') as f:
            dump = json.load(f)

        for file in self.db:

            item_name = pathlib.Path(file.path).as_posix()

            item = {
                item_name: {
                    'windows': str(pathlib.PureWindowsPath(file.path)),
                    'posix': pathlib.Path(file.path).as_posix(),
                }
            }

            if file.checksum:
                item[item_name][self.DVFile.checksum_name] = file.checksum

            if file.size:
                item[item_name]['size'] = file.size

            dump.update(item)

        with open(self.path, 'w') as f:
            json.dump(dump, f, indent=4)

    def add_folder(self, folder: str, filter: str = None):
        """ add all files in a folder to the database """
        for root, _, files in os.walk(folder):
            for file in files:
                if filter and isinstance(filter, str) and filter not in file:
                    continue
                file = self.DVFile(os.path.join(root, file))
                self.add_file(file=file)
        self.save()

    def add_file(self, file: DataValidationFile = None, path: str = None, checksum: str = None, size: int = None):
        """ add a validation file object to the database """

        if not file:
            file = self.DVFile(path=path, checksum=checksum, size=size)
        self.db.append(file)
        print(f'added {file.session.folder}/{file.name} to database (not saved)')

    #TODO update to classmethod like ShelveDB
    def get_matches(self,
                    file: DataValidationFile = None,
                    path: str = None,
                    size: int = None,
                    checksum: str = None,
                    match: int = None) -> List[DataValidationFile]:
        """search database for entries that match any of the given arguments 
        """
        if not file:
            file = self.DVFile(path=path, checksum=checksum, size=size)
        #! for now we only return equality of File(checksum + size)
        # or partial matches based on other input arguments

        if file and self.db.count(file):
            return [self.db.index(f) for f in self.db if f == file]

        elif path:
            name = os.path.basename(path)
            parent = pathlib.Path(path).parent.parts[-1]

            session_folder = Session.folder(path)

            if not size:
                size = os.path.getsize(path)

            # extract session_id from path if possible and add to comparison
            if size or checksum or (name and parent) or (session_folder and size):
                return [
                    self.db.index(f) \
                        for f in self.db \
                            if f.size == size or \
                                f.checksum == checksum or \
                                (f.name == name and f.parent == parent) or \
                                    (f.session_folder == session_folder and f.size == size)
                ]


class DataValidationFolder:

    db: Type[DataValidationDB] = None
    backups: List[str] = None

    def __init__(self, path: str):
        """ 
        represents a folder for which we want to checksum the contents and add to database
        possibly deleting if a valid copy exists elswhere
        """
        #* methods :
        #* __init__ check is folder, exists
        #*       possibly add all files in subfolders as DataValidationFile objects
        #* add_contents_to_database
        #* generate_large_file_checksums
        #*

        # extract the session ID from anywhere in the path
        self.session = Session(path)

        # ensure the path is a directory, not a file
        # if the file doesn't exist, we have to assume based on lack of file extension
        self.accessible = os.path.exists(path)
        if not self.accessible:
            if os.path.splitext(path)[1] == '':
                is_file = False
            else:
                is_file = True
        else:
            is_file = os.path.isfile(path)

        if is_file:
            raise ValueError(f"{self.__class__}: path must point to a folder {path=}")
        else:
            self.path = pathlib.Path(path).as_posix()

        # TODO lookup all possible locations for same session folder name

    def add_backup(self, path: Union[str, List[str]]):
        """Store one or more paths to folders containing backups for the session"""
        if path and isinstance(path, str):
            path = [path]
        elif path and isinstance(path, List) and path[0] != '': # inequality checks for str type and existence
            pass
        else:
            raise TypeError(f"{self.__class__}: path must be a string or list of strings")

        # add to list of backup locations as a Folder type object of the same class
        if not self.backups:
            self.backups = []
        for p in path:
            self.backups.append(self.__class__(p))

    def validate_backups(self, verbose: bool = True):
        """go through each file in the current folder (self) and look for valid copies in the backup folders"""
        if not self.backups:
            Warning(f"{self.__class__}: no backup locations specified")
            pass
        if not self.accessible:
            Warning(f"{self.__class__}: folder not accessible")
            # TODO implement standard file list for comparison without accessbile folder
            pass
        
        results = {}
        for root, _, files in os.walk(self.path):
            for f in files:
                # create new file object
                try:
                    file = self.db.DVFile(path=os.path.join(root, f))
                except [ValueError, TypeError]:
                    print(f"{self.__class__}: invalid file, not added to database: {f=}")
                    continue

                if not file.checksum:
                    file.checksum = file.generate_checksum(file.path)

                # check whether any similar files exist in current database
                matches, match_types = self.db.get_matches(file=file)

                # filter for matches with self and unrelated files
                hits = [x for i, x in enumerate(matches) if match_types[i] >= file.Match.VALID_COPY_SAME_NAME]

                # remove duplicates from hits (ie comparisons (file v other) that return the same value)
                extant_unique_hits = []
                while hits:
                    hit = hits.pop()
                    if hit.accessible:
                        for h in hits:
                            if (h.size == hit.size) \
                            and (h.path == hit.path) \
                            and (h.checksum == hit.checksum) \
                            :
                                hits.remove(h)
                        extant_unique_hits.append(hit)
                    else:
                        pass

                if extant_unique_hits:
                    results.update({file.name: [file.Match(file == f).name for f in extant_unique_hits]})
                    if verbose:
                        # print summary of file comparisons
                        report(file, extant_unique_hits)
                else:
                    results.update({file.name: "no matches"})
                    if verbose:
                        print(f"no matches found for {file.parent}/{file.name}")
                
            # aggregate results and print summary
            pprint.pprint(results)

    def add_folder_to_db(self, path: str = None, generate_large_checksums: bool = True):
        """add all contents of instance folder or a specified folder (all files in all subfiles) to the database
        """
        upper_size_limit = 1024**3 * 10 # 10GB - files above this won't have checksums generated unless generate_large_checksums == True

        if not self.db:
            raise ValueError(f"{self.__class__}: no database specified")

        if not path and self.accessible:
            # no folder specified, assume the current (self) folder is the target
            path = self.path
        elif path and os.path.exists(path):
            pass
        else:
            # no accessible folder supplied
            Warning(f"{self.__class__}: add_folder_to_db not implemented, not accessible: {path=}")
            pass

        for root, _, files in os.walk(path):
            for f in files:

                # create new file object
                try:
                    file = self.db.DVFile(path=os.path.join(root, f))
                except (ValueError, TypeError):
                    print(f"{self.__class__}: invalid file, not added to database: {file=}")
                    continue

                # check whether it exists in current database already
                matches, match_type = self.db.get_matches(file=file)

                #* move the following to some external function that applies a common logic for tasks
                # apply_strategy(file, strategy='add_to_db'/'delete_if_backed_up')
                if file.Match.SELF in match_type:
                    # we've re-checksummed the file and it matches a db entry
                    continue

                elif file.Match.OTHER_NO_CHECKSUM in match_type:
                    # we have a checksum, but the db entry doesnt:
                    self.db.add_file(file=file)
                    continue

                elif file.Match.SELF_NO_CHECKSUM in match_type:
                    # we have no checksum, but the db entry does:
                    #! choice here is to accept a possibly stale checksum or regenerate one
                    # TODO compare date of checksum in db to some age limit
                    # for now, we'll accept the db checksum if the file size is over a size threshold
                    if file.size > upper_size_limit and generate_large_checksums:
                        file.checksum = file.generate_checksum(file.path)
                        self.db.add_file(file=file)
                        continue
                    else:
                        # keep the checksum already in the db
                        continue

                elif file.checksum: # there are no entries in the db for this file with the same path
                    self.db.add_file(file=file)
                    continue

                elif file.size < file.checksum_threshold or generate_large_checksums:
                    file.checksum = file.generate_checksum(file.path)
                    self.db.add_file(file=file)
                    continue

                else:
                    # file is too large, no checksum generated
                    continue

    #TODO for implementation, add a backup database to check, to save generating twice
    def clear_dir(self, path: str):
        """Clear the contents of a folder if backups exist"""
        pass


class SyncDataValidationDB:

    DVDB: List[DataValidationDB] = None

    @classmethod
    def get_matches_in_all(file: DataValidationFile):
        # for each in list
        # check type matches DB file type,
        # call each db's 'get_matches' method and aggregate results
        raise NotImplementedError

    @classmethod
    def sync_all():
        # for each file in each db, grab all the files and add to all other dbs
        # - needs a get_all method in each db class
        #   - feed in number of entries to read -> transfer in pieces
        # - needs a
        raise NotImplementedError


def test_data_validation_file():
    """ test the data validation file class """

    class Test(DataValidationFile):

        def valid(path):
            return True

        checksum_generator = "12345678"
        checksum_test = None
        checksum_validate = valid

    cls = Test
    path = '/tmp/test.txt'
    checksum = '12345678'
    size = 10

    self = cls(path=path, checksum=checksum, size=size)

    other = cls(path=path, checksum=checksum, size=size)
    assert (self == self) == 5, "not recognized: self"

    other = cls(path='/tmp2/test.txt', checksum=checksum, size=size)
    assert (self == other) == 10, "not recgonized: valid copy, not self"

    other = cls(path='/tmp2/test2.txt', checksum=checksum, size=size)
    assert (self == other) == 11, "not recognized: valid copy, different name"

    other = cls(path='/tmp2/test.txt', checksum='87654321', size=20)
    assert (self == other) == 20, "not recognized: out-of-sync copy"

    other = cls(path='/tmp2/test.txt', checksum=checksum, size=20)
    assert (self == other) == 21, "not recognized: out-of-sync copy with incorrect checksum"
    #* note checksum is equal, which could occur if it hasn't been updated in db

    other = cls(path='/tmp2/test.txt', checksum='87654321', size=size)
    assert (self == other) == 22, "not recognized: corrupt copy"

    other = cls(path='/tmp/test2.txt', checksum=checksum, size=20)
    assert (self == other) == 30, "not recognized: checksum collision"

    other = cls(path='/tmp/test2.txt', checksum='87654321', size=20)
    assert (self == other) == 0, "not recognized: unrelated file"


test_data_validation_file()


def report(file: DataValidationFile, comparisons: List[DataValidationFile]):
    """ report on the contents of the folder, compared to database
        """
    # TODO write to a file in folder instead of printing to stdout

    if isinstance(comparisons, DataValidationFile):
        comparisons = [comparisons]

    column_width = 120 # for display of line separators

    def display_name(DVFile: DataValidationFile) -> str:
        min_len_filename = 50
        disp = f"{DVFile.parent}/{DVFile.name}"
        if len(disp) < min_len_filename:
            disp += ' ' * (min_len_filename - len(disp))
        return disp

    def display_str(label: str, DVFile: DataValidationFile) -> str:
        disp = f"{label} : {display_name(DVFile)} | {DVFile.checksum or '  none  '} | {DVFile.size or '??'} bytes"
        return disp

    print("#" * column_width)
    print("\n")
    print(f"subject: {file.path}")
    print("\n")
    print("-" * column_width)

    folder = file.path.split(file.name)[0]
    compare_folder = ""
    for other in comparisons:
        # print new header for each comparison with a new folder
        if compare_folder != other.path.split(other.name)[0]:
            compare_folder = other.path.split(other.name)[0]
            # print("*" * column_width)
            print("folder comparison for")
            print(f"subject : {folder}")
            print(f"other   : {compare_folder}")
            # print("*" * column_width)
            print("-" * column_width)

        print(f"Result  : {file.Match(file==other).name}")
        print(display_str("subject", file))
        print(display_str("other  ", other))
        print("-" * column_width)

    print("\n")
    print("#" * column_width)
