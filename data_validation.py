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

# to see large checksum performance (~400GB file)
db.DVFile.generate_checksum("//allen/programs/mindscope/production/incoming/recording_slot3_2.npx2")
"""

import abc
import dataclasses
import json
import os
import pathlib
import pdb
import re
import shelve
import sys
import tempfile
import zlib
from multiprocessing.sharedctypes import Value
from sqlite3 import dbapi2
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from warnings import WarningMessage


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

    def __init__(self, path: str):
        if not isinstance(path, str):
            raise TypeError(f"{self.__class__} path must be a string")

        self.folder = self.__class__.folder(path)

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
        if self.session_id == other.session_id:
            return self.relative_path < other.relative_path
        return self.session_id < other.session_id


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

        # ensure the path is a file, not directory
        # if the file doesn't exist, we have to assume based on lack of file extension
        if not os.path.exists(path):
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

        if path and not size and os.path.exists(path): # TODO replace exists check, race condition
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
            print(f"setting {self.checksum_name} checksum: {value}")
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
    
    def __eq__(self, other):
        # print("Testing checksum equality and filesize equality:")
        # if (self.checksum == other.checksum and self.size == other.size) \
        # or (self.checksum == other.checksum and self.size == other.size and self.path == other.path):
        if (self.checksum == other.checksum) \
            and (self.size == other.size) \
            and (self.path == other.path) \
            : # self
            return 5

        elif (self.checksum == other.checksum) \
            and (self.size == other.size) \
            and (self.name == other.name) \
            and (self.path != other.path) \
            : # valid copy, not self
            return 10

        elif (self.checksum == other.checksum) \
            and (self.size == other.size) \
            and (self.name != other.name) \
            and (self.path != other.path) \
            : # valid copy, different name
            return 11

        elif (self.name == other.name) \
            and (self.path != other.path) \
            : # invalid copy ( multiple categories)

            if (self.size != other.size) \
                and (self.checksum != other.checksum) \
                : # out-of-sync copy or incorrect data named as copy
                return 20
            
            if (self.size != other.size) \
                and (self.checksum == other.checksum) \
                : # out-of-sync copy or incorrect data named as copy
                # plus checksum which needs updating 
                # (different size with same checksum isn't possible)
                return 21

            if (self.size == other.size) \
                and (self.checksum != other.checksum) \
                : # possible data corruption, or checksum needs updating
                return 22

        elif (self.checksum == other.checksum) \
            and (self.size != other.size) \
            and (self.name != other.name) \
            : # possible checksum collision
            return 30

        else:      # apparently unrelated files (different name && checksum && size)
            return 40


class DataValidationFolder:

    def __init__(self, path: str):

        # if path is a file we can gain
        try:
            self.path = SessionFile(path).parent.as_posix()
        except ValueError:
            pass

    def add_backup_path(self, path: Union[str, List[str]]):
        """Store one or more paths to backup folders for the session"""
        pass

    def clear_dir(self, path: str):
        """Clear the contents of a folder if backups exist"""
        pass


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


class DataValidationFolder:
    """ 
    represents a folder for which we want to checksum the contents and add to database
    """
    #* connect to database
    #* methods :
    #* __init__ check is folder, exists
    #*       possibly add all files in subfolders as DataValidationFile objects
    #* add_contents_to_database
    #* generate_large_file_checksums
    #*


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
                    checksum: str = None) -> List[DataValidationFile]:
        """search database for entries that match any of the given arguments 
        """
        raise NotImplementedError


class ShelveDataValidationDB(DataValidationDB):
    """
    A database that stores data in a shelve database
    """
    DVFile: DataValidationFile = CRC32DataValidationFile
    db = "shelve_by_session"
    # key = session.folder

    @classmethod
    def add_file(cls, file: DataValidationFile):
        with shelve.open(cls.db, writeback=True) as db:
            if file.session.folder in db:
                db[file.session.folder].append(file)
            else:
                db[file.session.folder] = List(file)

    # @classmethod
    # def save(cls):
    #     self.db.sync()

    @classmethod
    def get_matches(cls,
                    file: DataValidationFile,
                    path: str = None,
                    size: int = None,
                    checksum: str = None) -> List[DataValidationFile]:
        """search database for entries that match any of the given arguments 
        """
        with shelve.open(cls.db, writeback=True) as db:
            if file.session.folder in db:
                return [f for f in db[file.session.folder] if file == f]

        # for key in self.db:
        #     if path is not None and key != path:
        #         continue
        #     if size is not None and self.db[key].size != size:
        #         continue
        #     if checksum is not None and self.db[key].checksum != checksum:
        #         continue
        #     matches.append(self.db[key])
        # return matches

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

    def get_matches(self,
                    file: DataValidationFile = None,
                    path: str = None,
                    size: int = None,
                    checksum: str = None) -> List[DataValidationFile]:
        """search database for entries that match any of the given arguments 
        """
        #! for now we only return equality of File(checksum + size)
        # or partial matches based on other input arguments

        # TODO return index of match/partial match, plus an enum (or similar) indicating the type of match (see DataValidationDB.__doc__)
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


def test_data_validation_file():
    """ test the data validation file class """
    class Test(DataValidationFile):
        def valid(path): return True
        checksum_generator = "12345678"
        checksum_test = None
        checksum_validate = valid

    cls = Test
    path = '/tmp/test.txt'
    checksum='12345678'
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
    assert (self == other) == 40, "not recognized: unrelated file"


test_data_validation_file()
