# -*- coding: utf-8 -*-
R"""Tools for validating neuropixels data files from ecephys recording sessions.

    Some design notes:
    - hash + filesize uniquely identify data, regardless of path 
    
    - the database holds previously-generated checksum hashes for
    large files (because they can take a long time to generate), plus their
    filesize at the time of checksum generation
    
    - small text-like files can have checksums generated on the fly
    so they don't need to live in the database (but they often do)
    
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
            
    - the basic unit for making these comparsons is a 'DataValidationFile' object, which has the properties above
    - checking the equality of two DVFile objects (ie subject == other) returns an enum specifying which of the relationships
      above is true
    - three or four parameters constitute a DataValidationFile object:
        -filepath
            -ecephys sessionID, which may be inferred from the filepath, 
            required for organization and many other possible uses of the file
        -checksum
        -size
    - not all of the parameters are required    
    - a standard baseclass template exists for connecting to a database, feeding-in file objects and getting matches
    - convenience / helper functions: live in a separate module ?

    
    Typical usage:

    x = DataValidationFileCRC32(
        path=
        R'\\allen\programs\mindscope\workgroups\np-exp\1190290940_611166_20220708\1190258206_611166_20220708_surface-image1-left.png'
    )
    print(f'checksum is auto-generated for small files: {x.checksum}')

    y = DataValidationFileCRC32(
        checksum=x.checksum, 
        size=x.size, 
        path='/dir/1190290940_611166_20220708_foo.png'
    )

    # DataValidationFile objects evaulate to True if they have some overlap between filename (regardless of path),
    # checksum, and size: 
    print(x == y)

    # only files that are unrelated, and have no overlap in filename, checksum, or size,
    # evaluate to False
    
    # connecting to a database:
    db = MongoDataValidationDB()
    db.add_file(x)
    
    # to see large-file checksum performance (~400GB file)
    db.DVFile.generate_checksum('//allen/programs/mindscope/production/incoming/recording_slot3_2.npx2)

    # applying to folders
    local = R'C:\Users\ben.hardcastle\Desktop\1190258206_611166_20220708'
    npexp = R'\\w10dtsm18306\neuropixels_data\1190258206_611166_20220708'
    f = dv.DataValidationFolder(local)
    f.db = dv.MongoDataValidationDB
    f.add_folder_to_db(local)
    f.add_folder_to_db(npexp)

    f.add_backup(npexp)

    f.validate_backups(verbose=True)
"""

import abc
import enum
import json
import logging
import logging.handlers
import mmap
import os
import pathlib
import re
import shelve
import sys
import tempfile
import traceback
import zlib
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union

try:
    import pymongo
except ImportError:
    print("pymongo not installed")
    
import data_getters as dg  # from corbett's QC repo

# LOG_DIR = fR"//allen/programs/mindscope/workgroups/np-exp/ben/data_validation/logs/"
logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", filename="data_validation.log", level=logging.DEBUG,datefmt="%Y-%m-%d %H:%M")
log = logging.getLogger(__name__)
logHandler = logging.handlers.RotatingFileHandler('data_validation.log', maxBytes=10000, backupCount=5)
log.addHandler(logHandler)
log.setLevel(logging.DEBUG)


def error(e: TypeError) -> str:
    return ''.join(traceback.TracebackException.from_exception(e).format())


def progressbar(it,
                prefix="",
                size=40,
                file=sys.stdout,
                units: str = None,
                unit_scaler: int = None,
                display: bool = True):
    # from https://stackoverflow.com/a/34482761
    count = len(it)
    display=False
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


def chunk_crc32(file:Any=None, fsize=None) -> str:
    """ generate crc32 with for loop to read large files in chunks """
    if isinstance(file, str):
        pass
    elif isinstance(file, type(pathlib.Path)):
        file = str(file)
    elif isinstance(file, DataValidationFile):
        file = file.path
        fsize = file.size

    chunk_size = 65536 # bytes

    # print('using builtin ' + inspect.stack()[0][3])

    # get filesize just once
    if not fsize:
        fsize = os.stat(file).st_size

    # don't show progress bar for small files
    display = True if fsize > 10 * chunk_size else False

    crc = 0
    with open(str(file), 'rb', chunk_size) as ins:
        for _ in progressbar(range(int((fsize / chunk_size)) + 1),
                             prefix="generating crc32 checksum ",
                             units="B",
                             unit_scaler=chunk_size,
                             display=display):
            crc = zlib.crc32(ins.read(chunk_size), crc)

    return '%08X' % (crc & 0xFFFFFFFF)

def mmap_direct(fpath: Union[str, pathlib.Path], fsize=None) -> str:
    """ generate crc32 with for loop to read large files in chunks """
    # print('using standalone ' + inspect.stack()[0][3])
    print(f'using mmap_direct for {fpath}')
    crc = 0
    with open(str(fpath), 'rb') as ins:
        with mmap.mmap(ins.fileno(), 0, access=mmap.ACCESS_READ) as m:
            crc = zlib.crc32(m.read(), crc)
    return '%08X' % (crc & 0xFFFFFFFF)

def test_crc32_function(func, *args, **kwargs):
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
    
    NPEXP_ROOT = pathlib.Path(R"//allen/programs/mindscope/workgroups/np-exp")

    def __init__(self, path: str):
        if not isinstance(path, str):
            raise TypeError(f"{self.__class__} path must be a string")

        self.folder = self.__class__.folder(path)
        # TODO maybe not do this - could be set to class without realizing - just assign for instances
        self.lims_path = self.__class__.lims_path(path)
        self.npexp_path = self.__class__.npexp_path(path)

        if self.folder:
            # extract the constituent parts of the session folder
            self.id = self.folder.split('_')[0]
            self.mouse = self.folder.split('_')[1]
            self.date = self.folder.split('_')[2]
        else:
            raise ValueError(f"{self.__class__} path must contain a valid session folder")

    @classmethod
    def folder(cls, path) -> Union[str, None]:
        """Extract [10-digit session ID]_[6-digit mouse ID]_[6-digit date
        str] from a file or folder path"""

        # identify a session based on
        # [10-digit session ID]_[6-digit mouseID]_[6-digit date str]
        session_reg_exp = "[0-9]{0,10}_[0-9]{0,6}_[0-9]{0,8}"

        session_folders = re.findall(session_reg_exp, path)
        if session_folders:
            if not all(s == session_folders[0] for s in session_folders):
                logging.warning(f"{cls.__class__} Mismatch between session folder strings - file may be in the wrong flder: {path}")
            return session_folders[0]
        else:
            return None
        
    @classmethod
    def npexp_path(cls, path) -> Union[str, None]:
        '''get session folder from path/str and combine with npexp root to get folder path on npexp'''        
        folder = cls.folder(path)
        if not folder:
            return None
        return cls.NPEXP_ROOT / folder

    @classmethod
    def lims_path(cls, path) -> Union[str, None]:
        '''get lims id from path/str and lookup the corresponding directory in lims'''
        folder = cls.folder(path)
        if not folder:
            return None
        
        try:
            lims_dg = dg.lims_data_getter(cls(path).id)
            WKF_QRY =   '''
                        SELECT es.storage_directory
                        FROM ecephys_sessions es
                        WHERE es.id = {}
                        '''
            lims_dg.cursor.execute(WKF_QRY.format(lims_dg.lims_id))
            exp_data = lims_dg.cursor.fetchall()
            if exp_data and exp_data[0]['storage_directory']:
                return str('/'+exp_data[0]['storage_directory'])
            else:
                return None
            
        except:
            return None


class SessionFile:
    """ Represents a single file belonging to a neuropixels ecephys session """

    session = None

    def __init__(self, path: str):
        """ from the complete file path we can extract some information upon
        initialization """

        if not isinstance(path, (str, pathlib.Path)):
            raise TypeError(f"{self.__class__}: path must be a str pointing to a file: {type(path)}")
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
            raise ValueError(f"{self.__class__}: path must point to a file {path}")
        else:
            self.path = path

        self.name = os.path.basename(self.path)

        # get the name of the folder the file lives in (which may be the same as self.root_path below)
        self.parent = pathlib.Path(os.path.dirname(self.path)).parts[-1]

        # extract the session ID from anywhere in the path
        self.session = Session(self.path)
        if not self.session:
            raise ValueError(f"{self.__class__}: path does not contain a session ID {path}")
    
    @property
    def root_path(self) -> str:
        """root path of the file (may be the same as session_folder_path)"""
        # we expect the session_folder string to first appear in the path as
        # a child of some 'repository' of session folders (like npexp), 
        # - split the path at the first session_folder match and call that folder the root
        parts = pathlib.Path(self.path).parts
        while parts:
            if self.session.folder in parts[0]:
                break
            parts = parts[1:]
        else:
            raise ValueError(f"{self.__class__}: session_folder not found in path {self.path}")
        
        return self.path.split(str(parts[0]))[0]


    @property
    def session_folder_path(self) -> Union[str, None]:
        """path to the session folder, if it exists"""
        
        # if a repository (eg npexp) contains session folders, the following location should exist:
        session_folder_path = os.path.join(self.root_path, self.session.folder)
        if os.path.exists(session_folder_path):
            return session_folder_path

        # but it might not exist: we could have a file sitting in a folder with a flat structure:
        # assorted files from multiple sessions in a single folder (e.g. LIMS incoming),
        # or a folder which has the session_folder pattern plus extra info
        # appended, eg. _probeABC
        else:
            self.session_folder_path = None
    
    
    @property
    def session_relative_path(self) -> Union[str, None]:
        '''filepath relative to a session folder's parent'''
        # wherever the file is, get its path relative to the parent of a
        # hypothetical session folder ie. session_id/.../filename.ext :
        session_relative_path = pathlib.Path(self.path).relative_to(self.root_path)
        if session_relative_path.parts[0] != self.session.folder:
            return os.path.join(self.session.folder, str(session_relative_path))
        else:
            return str(session_relative_path)
    
    
    @property
    def npexp_path(self) -> Union[str, None]:
        '''filepath on npexp (might not exist)'''
        if self.session:
            return self.session.NPEXP_ROOT / self.session_relative_path
        else:
            return None
    
    def __lt__(self, other):
        if self.session.id == other.session.id:
            return self.session_relative_path < other.session_relative_path
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
            raise TypeError(f"{self.__class__}: path must be a str pointing to a file: {type(path)}")

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
            raise ValueError(f"{self.__class__}: path must point to a file {path}")
        else:
            self.path = pathlib.Path(path).as_posix()

        # we have a mix in the databases of posix paths with and without the double fwd slash
        if self.path[0] == '/' and self.path[1] != '/':
            self.path = '/' + self.path
            
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
            self.checksum = self.__class__.generate_checksum(self.path, self.size) # change to use instance method if available

    @classmethod
    def generate_checksum(cls, path, size=None) -> str:
        cls.checksum_test(cls.checksum_generator)
        return cls.checksum_generator(path, size)
    # def generate_checksum(self) -> str:
    #     self.checksum_test(functools.partialmethod(self.checksum_generator(), self.path))
    #     return self.checksum_generator()

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
        """Integer enum for DataValidationFile equality - test for (file==other)>0 for
        matches of interest and >20 for valid backups"""
        UNRELATED = 0
        UNKNOWN = -1
        SELF = 5
        #! watch out: SELF_NO_CHECKSUM and OTHER_NO_CHECKSUM
        # depend on the order of objects in the inequality
        SELF_NO_CHECKSUM = 6
        OTHER_NO_CHECKSUM = 7
        CHECKSUM_COLLISION = 10
        UNSYNCED_DATA = 11
        UNSYNCED_CHECKSUM = 12
        UNSYNCED_OR_CORRUPT_DATA = 13
        VALID_COPY_SAME_NAME = 21
        VALID_COPY_RENAMED = 22

    def __eq__(self, other):
        """Test equality of two DataValidationFile objects"""
        # size and path fields are required entries in a DVF entry in database -
        # checksum is optional, so we need to check for it in both objects
        if self.checksum and other.checksum \
            and (self.checksum == other.checksum) \
            and (self.size == other.size) \
            and (self.path.lower() == other.path.lower()) \
            : # self
            return self.__class__.Match.SELF.value

        #! watch out: SELF_NO_CHECKSUM and OTHER_NO_CHECKSUM
        # depend on the order of objects in the inequality
        elif (self.size == other.size) \
            and (self.path.lower() == other.path.lower()) \
            and (not self.checksum) \
            and (other.checksum) \
            : # self without checksum confirmation (self missing)
            return self.__class__.Match.SELF_NO_CHECKSUM.value
        #! watch out: SELF_NO_CHECKSUM and OTHER_NO_CHECKSUM
        # depend on the order of objects in the inequality
        elif (self.size == other.size) \
            and (self.path.lower() == other.path.lower()) \
            and (self.checksum) \
            and not (other.checksum) \
            : # self without checksum confirmation (other missing)
            return self.__class__.Match.OTHER_NO_CHECKSUM.value

        elif self.checksum and other.checksum \
            and (self.checksum == other.checksum) \
            and (self.size == other.size) \
            and (self.name.lower() == other.name.lower()) \
            and (self.path.lower() != other.path.lower()) \
            : # valid copy, not self
            return self.__class__.Match.VALID_COPY_SAME_NAME.value

        elif self.checksum and other.checksum \
            and (self.checksum == other.checksum) \
            and (self.size == other.size) \
            and (self.name.lower() != other.name.lower()) \
            and (self.path.lower() != other.path.lower()) \
            : # valid copy, different name
            return self.__class__.Match.VALID_COPY_RENAMED.value

        elif self.checksum and other.checksum \
            and (self.name.lower() == other.name.lower()) \
            and (self.path.lower() != other.path.lower()) \
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
                return self.__class__.Match.UNSYNCED_OR_CORRUPT_DATA.value

        elif self.checksum and other.checksum \
            and (self.checksum == other.checksum) \
            and (self.size != other.size) \
            and (self.name.lower() != other.name.lower()) \
            : # possible checksum collision
            return self.__class__.Match.CHECKSUM_COLLISION.value

        elif self.checksum and other.checksum \
            and (self.checksum != other.checksum) \
            and (self.size != other.size) \
            and (self.name.lower() != other.name.lower()) \
            :# apparently unrelated files (different name && checksum && size)
            return self.__class__.Match.UNRELATED.value

        else:      # insufficient information
            return self.__class__.Match.UNKNOWN.value


class CRC32DataValidationFile(DataValidationFile, SessionFile):

    # DB: DataValidationDB = CRC32JsonDataValidationDB()
    checksum_threshold: int = 0 # don't generate checksum for any files by default
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
        # try:
        SessionFile.__init__(self, path)
        DataValidationFile.__init__(self, path=path, checksum=checksum, size=size)
        # except AttributeError:
        #     print(f"{self.__class__}: no session dir in path")
        #     clear(self)
        #     return

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
    db = "//allen/programs/mindscope/workgroups/np-exp/ben/data_validation/db/shelve_by_session_id"

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
                (
                    [x for x in db[key] if (file == x) == cls.DVFile.Match.SELF] \
                    or [x for x in db[key] if (file == x) == cls.DVFile.Match.SELF_NO_CHECKSUM] \
                ):
                print(f'skipped {file.session.folder}/{file.name} in Shelve database')
                return

            if key in db:
                db[key].append(file)
            else:
                db[key] = [file]

            print(f'added {file.session.folder}/{file.name} to Shelve database')

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
            return [o for o in matches if (file == o) == match > 0], \
                [(file == o) for o in matches if (file == o) == match > 0]
        else:
            return [o for o in matches if (file == o) > 0], \
                [(file == o) for o in matches if (file == o) > 0]

    # def __del__(self):
    #     self.db.close()


class MongoDataValidationDB(DataValidationDB):
    """
    A database that stores validation data in mongodb 
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

        # check an identical entry doesn't exist already
        _, match_type = cls.get_matches(file)
        if (cls.DVFile.Match.SELF in match_type) \
            or (cls.DVFile.Match.SELF_NO_CHECKSUM in match_type):
            print(f'skipped {file.session.folder}/{file.name} in Mongo database')
            return

        cls.db.insert_one({
            "session_id": file.session.id,
            "path": file.path,
            "checksum": file.checksum,
            "size": file.size,
            "type": file.checksum_name,
        })
        print(f'added {file.session.folder}/{file.name} to Mongo database')

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
            # "path": re.compile(file.name),
        }))
        # TODO get rid of duplicates here before creating DV file objects 
        # matches = []
        # for entry in entries:
        #     matches.append(
        #         cls.DVFile(
        #         path=entry['path'],
        #         checksum=entry['checksum'],
        #         size=entry['size'],
        #     )
        #     )
                           
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

            return [o for o in matches if (file == o) == match > 0], \
                [(file == o) for o in matches if (file == o) == match > 0]

        else:
            return [o for o in matches if (file == o) > 0], \
                [(file == o) for o in matches if (file == o) > 0]

    # def __del__(self):
    #     self.db.close()


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

                    if 'linux' in keys:
                        path = items[item]['linux']
                    elif 'posix' in keys:
                        path = items[item]['posix']
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
        print(f'added {file.session.folder}/{file.name} to json database (not saved)')

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
            return [self.db.index(f) for f in self.db if file == f]

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
    backup_paths: Set[str] = set()
    generate_large_checksums: bool = True
    regenerate_large_checksums: bool = False
    include_subfolders: bool = True
    upper_size_limit = 1024**3 * 5 # GB - files above this won't have checksums generated unless generate_large_checksums == True

    def __init__(self, path: str):
        """ 
        represents a folder for which we want to checksum the contents and add to database
        possibly deleting if a valid copy exists elswhere
        """
        #* methods :
        #* __init__ check is folder, exists
        #*       possibly add all files in subfolders as DataValidationFile objects
        #* add_contents_to_database
        #* generate_large_checksums
        #*

        # extract the session ID from anywhere in the path (not reqd)
        try:
            self.session = Session(path)
        except:
            pass

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
            raise ValueError(f"{self.__class__}: path must point to a folder {path}")
        else:
            self.path = pathlib.Path(path).as_posix()

        # TODO lookup all possible locations for same session folder name

    def add_backup(self, path: Union[str, List[str]]):
        """Store one or more paths to folders containing backups for the session"""
        if path and (isinstance(path, str) or isinstance(path, pathlib.Path)):
            path = [str(path)]
        elif path and isinstance(path, List): # inequality checks for str type and existence
            pass
        else:
            raise TypeError(f"{self.__class__}: path must be a string or list of strings")
            # add to list of backup locations as a Folder type object of the same class
        for p in path:
            if str(p) != '':
                self.backup_paths.add(str(p))

    def backups(self) -> List[str]:
        """Return a list of Folder objects containing backups for the session"""
        return list(self.backup_paths)

    def validate_backups(self,
                         verbose: bool = True,
                         log_dir: str = "//allen/programs/mindscope/workgroups/np-exp/ben/data_validation/logs",
                         delete: bool = False,
                         ):
        """go through each file in the current folder (self) and look for valid copies in the backup folders"""
        if not self.backups():
            print(
                f"{self.__class__}: no backup locations specified - use 'folder.add_backup(path)' to add one or more backup locations"
            )
            return
        if not self.accessible:
            print(f"{self.__class__}: folder not accessible")
            # TODO implement standard file list for comparison without accessbile folder
            return

        results = {}
        for root, _, files in os.walk(self.path):
            for f in files:

                # create new file object
                try:
                    file = self.db.DVFile(path=os.path.join(root, f))
                except [ValueError, TypeError]:
                    print(f"{self.__class__}: invalid file, not added to database: {f}")
                    continue

                # generate new checksums for large files if toggled on
                if not file.checksum \
                    and (self.regenerate_large_checksums \
                        or file.size < self.upper_size_limit) \
                    :
                    file.checksum = file.generate_checksum(file.path)
                    self.db.add_file(file)

                # check in current database for similar files
                matches, match_types = self.db.get_matches(file=file)

                if not file.checksum \
                    and file.Match.SELF_NO_CHECKSUM in match_types:

                    # we have entries in db with checksums already generated - we can replace
                    # the file with the db entry

                    # TODO here we assume that all matching entries have the same checksum
                    #* we take the last match, hoping they were added to db chronologically
                    #* but we really want to check a date field to find the most recent matching checksum entry
                    others_with_checksum = [
                        x for i, x in enumerate(matches) if match_types[i] == file.Match.SELF_NO_CHECKSUM
                    ]
                    if not all(owc.checksum == others_with_checksum[-1].checksum \
                        for owc in others_with_checksum):
                        logging.warning(
                            f"{self.__class__}: Skipped: {file.path} multiple db entries with different checksums")
                        continue
                    else:
                        file = others_with_checksum[-1]

                # generate checksums for large files only if toggled on
                elif not file.checksum \
                    and (file.size < self.upper_size_limit \
                        or self.generate_large_checksums) \
                    :
                    file.checksum = file.generate_checksum(file.path)
                    self.db.add_file(file)

                elif not file.checksum:
                    print(f"{self.__class__}: {file.path} large file, not validated")
                    continue

                # check again in current database for similar files
                matches, match_types = self.db.get_matches(file=file)
                # now filter for matches with self and unrelated files
                hits = [x for i, x in enumerate(matches) if match_types[i] >= file.Match.CHECKSUM_COLLISION]

                if not hits:
                    # check backup for similar files
                    for b_folder in self.backups():
                        b = DataValidationFolder(b_folder)
                        if b.accessible:
                            b_file = pathlib.Path(b.path / pathlib.Path(file.path).relative_to(self.path))
                            if b_file.exists():

                                bb = self.db.DVFile(path=b_file.as_posix())
                                bb.checksum = bb.generate_checksum(bb.path)
                                self.db.add_file(bb)

                            else:
                                size_match = [
                                    bbb
                                    for bbb in pathlib.Path(b.path).glob('*')
                                    if not bbb.is_dir() and (file.size == os.path.getsize(bbb))
                                ]
                                if size_match:
                                    for sm in size_match:
                                        bb = self.db.DVFile(path=sm.as_posix())
                                        bb.checksum = bb.generate_checksum(bb.path)
                                        self.db.add_file(bb)

                # check again in current database for similar files
                matches, match_types = self.db.get_matches(file=file)
                # now filter for matches with self and unrelated files
                hits = [x for i, x in enumerate(matches) if match_types[i] >= file.Match.CHECKSUM_COLLISION]

                # remove duplicates from hits (ie comparisons (file v other) that return the same value)
                unique_hits = []
                while hits:
                    hit = hits.pop()
                    for h in hits:
                        if (h.size == hit.size) \
                        and (h.path == hit.path) \
                        and (h.checksum == hit.checksum) \
                        :
                            hits.remove(h)
                    unique_hits.append(hit)

                extant_unique_hits = [x for x in unique_hits if x.accessible]

                # def add_to_results(results,file,other,label):
                #     datestr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                #     results.update(
                #         {f'matches in {backup}': {
                #             datestr: file.path,
                #             file.Match(file == euh).name: euh.path
                #         }})

                # if not unique_hits:
                #     for uh in unique_hits:
                #         add_to_results(results, file, uh, "no copies found in db")
                #     if verbose:
                #         print(f"no backups found in db for {file.parent}/{file.name}")
                #     continue # no verifiable backups found

                # for backup in self.backups():
                #     for euh in extant_unique_hits:
                #         # check if the entry's path is in the backup folder
                #         if backup in euh.path:
                #             add_to_results(results, file, euh, "accessible backups found")

                #         elif not any(x in euh.path for x in self.backups()):
                #             add_to_results(results, file, euh, "other copies found")

                if verbose and '*.dat' in file.path or '*.npx2' in file.path:
                    # print summary of file comparisons
                    report(file, extant_unique_hits)
                if delete:
                    for backup in self.backups():
                        column_width = 120 # for display of line separators
                        def display_name(DVFile: DataValidationFile) -> str:
                            min_len_filename = 80
                            disp = f"{DVFile.path}"
                            if len(disp) < min_len_filename:
                                disp += ' ' * (min_len_filename - len(disp))
                            return disp
                        def display_str(label: str, DVFile: DataValidationFile) -> str:
                            disp = f"{label} : {display_name(DVFile)} | {DVFile.checksum or '  none  '} | {DVFile.size or '??'} bytes"
                            return disp
                        for euh in extant_unique_hits:
                            # check if the entry's path is in the backup folder
                            if backup in euh.path and (file == euh) >= file.Match.VALID_COPY_SAME_NAME:

                                # delete the file from the Validation folder                            
                                if os.path.exists(file.path):
                                    logging.info(display_str(f"{file.Match.SELF.name} (DELETED)", file))
                                    pathlib.Path(file.path).unlink()
                                    
                                # report on extant backup:
                                logging.info(display_str(f"{file.Match(file==euh).name}", euh))

                                # return

        # # aggregate results and write or print summary
        # if results and log_dir and isinstance(log_dir,str):
        #     pathlib.Path(log_dir).mkdir(exist_ok=True, parents=True)
        #     log_path = f"{log_dir}/{file.session.folder}-backup_validation_log.yaml"
        #     # if os.path.exists(log_path):
        #     #     with open(log_path,'r') as j:
        #     #         r = yaml.load(j)
        #     #         results = {**r, **results}
        #     with open(log_path, 'w') as j:
        #         yaml.dump(results, j, sort_keys=True, default_flow_style=False)
        # elif results:
        #     pprint.pprint(results)

    def add_folder_to_db(self, path: str = None) -> List[DataValidationFile]:
        """add all contents of instance folder or a specified folder (all files in all subfiles) to the database
        """

        if not self.db:
            raise ValueError(f"{self.__class__}: no database specified")

        if not path and self.accessible:
            # no folder specified, assume the current (self) folder is the target
            path = self.path
        elif path and os.path.exists(path):
            pass
        else:
            # no accessible folder supplied
            print(f"{self.__class__}: add_folder_to_db not implemented, not accessible: {path}")
            return

        file_objects = []
        if self.include_subfolders:
            files = pathlib.Path(path).rglob('*')
        else:
            files = [child for child in pathlib.Path(path).iterdir() if not child.is_dir()]

        for f in files:
            if isinstance(f, str):
                f = pathlib.Path(f)

            # create new file object
            try:
                file = self.db.DVFile(path=f.as_posix())
            except (ValueError, TypeError):
                print(f"{self.__class__}: invalid file, not added to database: {f.as_posix()}")
                continue

            # check whether it exists in current database already
            matches, match_type = self.db.get_matches(file=file)

            def add(f):
                '''add file to database'''
                self.db.add_file(file=f)
                file_objects.append(f)

            def gen(f):
                '''generate checksum for file and add to database'''
                print(f.path)
                f.checksum = f.generate_checksum(f.path,f.size)
                add(f)

            #* move the following to some external function that applies a common logic for tasks
            # apply_strategy(file, strategy='add_to_db'/'delete_if_backed_up')
            if file.Match.SELF in match_type:
                # we've re-checksummed the file and it matches a db entry
                continue

            elif file.Match.OTHER_NO_CHECKSUM in match_type:
                # we have a checksum, but the db entry doesnt:
                add(file)
                continue
            
            elif file.Match.SELF_NO_CHECKSUM in match_type:
                # we have no checksum, but the db entry does:
                #! choice here is to accept a possibly stale checksum or regenerate one
                # TODO compare date of checksum in db to some age limit
                if file.size > self.upper_size_limit \
                    or not self.regenerate_large_checksums \
                    : # accept the db checksum if the file size is over a size threshold
                    continue
                else:
                    # regenerate the checksum and add to db
                    gen(file)
                    continue

            elif not matches and file.checksum:
                # there are no entries in the db for this filepath
                add(file)
                continue
            elif not matches and (file.size < self.upper_size_limit or self.generate_large_checksums):
                gen(file)
                continue
            else:
                # file is too large, no checksum generated
                logging.info(f"{self.__class__}: file too large, not added to database: {file.path}")
                continue

        return file_objects

    #TODO for implementation, add a backup database to check, to save generating twice
    def clear_dir(self, path: str):
        """Clear the contents of a folder if backups exist"""
        pass



def test_data_validation_file():
    """ test the data validation file class """

    class Test(DataValidationFile):

        def valid(path):
            return True

        checksum_generator = "12345678"
        checksum_test = None
        checksum_validate = valid

    cls = Test
    path = '//tmp/tmp/test.txt' # currently only working with network drives, which require a folder in the middle between drive/file
    checksum = '12345678'
    size = 10

    self = cls(path=path, checksum=checksum, size=size)

    other = cls(path=path, checksum=checksum, size=size)
    assert (self == self) == self.Match.SELF, "not recognized: self"

    other = cls(path='//tmp2/tmp/test.txt', checksum=checksum, size=size)
    assert (self == other) == self.Match.VALID_COPY_SAME_NAME, "not recgonized: valid copy, not self"

    other = cls(path='//tmp2/tmp/test2.txt', checksum=checksum, size=size)
    assert (self == other) == self.Match.VALID_COPY_RENAMED, "not recognized: valid copy, different name"

    other = cls(path='//tmp2/tmp/test.txt', checksum='87654321', size=20)
    assert (self == other) == self.Match.UNSYNCED_DATA, "not recognized: out-of-sync copy"

    other = cls(path='//tmp2/tmp/test.txt', checksum=checksum, size=20)
    assert (self == other) == self.Match.UNSYNCED_CHECKSUM, "not recognized: out-of-sync copy with incorrect checksum"
    #* note checksum is equal, which could occur if it hasn't been updated in db

    other = cls(path='//tmp2/tmp/test.txt', checksum='87654321', size=size)
    assert (self == other) == self.Match.UNSYNCED_OR_CORRUPT_DATA, "not recognized: corrupt copy"

    other = cls(path='//tmp/tmp/test2.txt', checksum=checksum, size=20)
    assert (self == other) == self.Match.CHECKSUM_COLLISION, "not recognized: checksum collision"

    other = cls(path='//tmp/tmp/test2.txt', checksum='87654321', size=20)
    assert (self == other) == self.Match.UNRELATED, "not recognized: unrelated file"


test_data_validation_file()



def report(file: DataValidationFile, comparisons: List[DataValidationFile]):
    """ report on the contents of the folder, compared to database
        """
    if isinstance(comparisons, DataValidationFile):
        comparisons = [comparisons]

    column_width = 120 # for display of line separators

    def display_name(DVFile: DataValidationFile) -> str:
        min_len_filename = 80
        disp = f"{DVFile.parent}/{DVFile.name}"
        if len(disp) < min_len_filename:
            disp += ' ' * (min_len_filename - len(disp))
        return disp

    def display_str(label: str, DVFile: DataValidationFile) -> str:
        disp = f"{label} : {display_name(DVFile)} | {DVFile.checksum or '  none  '} | {DVFile.size or '??'} bytes"
        return disp

    logging.info("#" * column_width)
    logging.info("\n")
    logging.info(f"subject: {file.path}")
    logging.info("\n")
    logging.info("-" * column_width)

    folder = file.path.split(file.name)[0]
    compare_folder = ""
    for other in comparisons:
        # logging.info new header for each comparison with a new folder
        if compare_folder != other.path.split(other.name)[0]:
            compare_folder = other.path.split(other.name)[0]
            # logging.info("*" * column_width)
            logging.info("folder comparison for")
            logging.info(f"subject : {folder}")
            logging.info(f"other   : {compare_folder}")
            # logging.info("*" * column_width)
            logging.info("-" * column_width)

        logging.info(f"Result  : {file.Match(file==other).name}")
        logging.info(display_str("subject", file))
        logging.info(display_str("other  ", other))
        logging.info("-" * column_width)

    logging.info("\n")
    logging.info("#" * column_width)

