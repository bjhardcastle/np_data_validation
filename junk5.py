import inspect
import mmap
import os
import pathlib
import sys
import timeit
import zlib
from ast import Assert
from typing import Any, List, Union

import data_validation as dv

large = "//allen/programs/braintv/production/incoming/neuralcoding/1170788301_607186_20220414_probeDEF/recording_slot3_2.npx2"
med = R"\\allen\programs\braintv\production\incoming\neuralcoding\1120251668_578004_20210805_probeD_sorted\continuous\Neuropix-PXI-100.0\amplitudes.npy"


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


def chunk_crc32(fpath: Any = None, fsize=None) -> str:
    """ generate crc32 with for loop to read large fpaths in chunks """
    if isinstance(fpath, str):
        pass
    elif isinstance(fpath, type(pathlib.Path)):
        fpath = str(fpath)
    elif isinstance(fpath, dv.DataValidationFile):
        fpath = fpath.path
        fsize = fpath.size

    chunk_size = 8 * 65536 # bytes

    # don't show progress bar for small files
    display = True if os.stat(fpath).st_size > 10 * chunk_size else False

    print('using standalone ' + inspect.stack()[0][3])

    # get filesize just once
    if not fsize:
        fsize = os.stat(fpath).st_size

    crc = 0
    with open(str(fpath), 'rb', chunk_size) as ins:

        for _ in progressbar(range(int((fsize / chunk_size)) + 1),
                             prefix="generating crc32 checksum ",
                             units="B",
                             unit_scaler=chunk_size,
                             display=display):
            crc = zlib.crc32(ins.read(chunk_size), crc)

    return '%08X' % (crc & 0xFFFFFFFF)


def mmap_crc32(fpath: Union[str, pathlib.Path], fsize=None) -> str:
    """ generate crc32 with for loop to read large files in chunks """
    chunk_size = 1* 65536 # bytes
                              # don't show progress bar for small files
    display = True            #if os.stat(fpath).st_size > 10 * chunk_size else False

    print('using standalone ' + inspect.stack()[0][3])

    crc = 0
    if not fsize:
        fsize = os.stat(fpath).st_size                                 # bytes
    with open(str(fpath), 'rb',chunk_size) as ins:
        for _ in progressbar(range(int((fsize / chunk_size)) + 1),
                                prefix="generating crc32 checksum ",
                                units="B",
                                unit_scaler=chunk_size,
                                display=display):
            with mmap.mmap(ins.fileno(), 0, access=mmap.ACCESS_READ) as m:
                crc = zlib.crc32(m.read(), crc)
    return '%08X' % (crc & 0xFFFFFFFF)


def mmap_direct(fpath: Union[str, pathlib.Path], fsize=None) -> str:
    """ generate crc32 with for loop to read large files in chunks """
    # chunk_size = 65536 # bytes
    # don't show progress bar for small files
    display = True #if os.stat(fpath).st_size > 10 * chunk_size else False
                   # if not fsize:
                   #     fsize = os.stat(fpath).st_size  # bytes

    print('using standalone ' + inspect.stack()[0][3])

    crc = 0
    with open(str(fpath), 'rb') as ins:
        with mmap.mmap(ins.fileno(), 0, access=mmap.ACCESS_READ) as m:
            # for _ in progressbar(range(int((os.stat(fpath).st_size / chunk_size)) + 1),
            #                      prefix="generating crc32 checksum ",
            #                      units="B",
            #                      unit_scaler=chunk_size,
            #                      display=display):
            crc = zlib.crc32(m.read(), crc)
    return '%08X' % (crc & 0xFFFFFFFF)


f = med
file = dv.CRC32DataValidationFile(path=f)
# fsize = os.stat(f).st_size  # bytes


def standard():
    dv.CRC32DataValidationFile.checksum_generator = chunk_crc32
    try:
        print(file.generate_checksum(file.path))
    except AssertionError:
        print('Invalid checksum')


def mm():
    dv.CRC32DataValidationFile.checksum_generator = mmap_crc32
    try:
        print(file.generate_checksum(file.path))
    except AssertionError:
        print('Invalid checksum')


def mm_direct():
    dv.CRC32DataValidationFile.checksum_generator = mmap_direct
    try:
        print(file.generate_checksum(file.path))
    except AssertionError:
        print('Invalid checksum')


N = 1
results = {}


def add_results(name, time):
    results[name] = f"{time:.2f} s"

i=0
# t = timeit.timeit(standard, number=N)
# add_results("2.1_standard:",t)
# t = timeit.timeit(mm_direct, number=N)
# add_results("2.1_mm_direct:", t)
# t = timeit.timeit(mm_direct, number=N)
# add_results("2.3_mm_direct:", t)
t = timeit.timeit(mm_direct, number=N)
i+=1
add_results(f"{i}_mm:", t)
t = timeit.timeit(standard, number=N)
i+=1
add_results(f"{i}_standard:", t)
t = timeit.timeit(standard, number=N)
i+=1
add_results(f"{i}_standard:", t)

import pprint

pprint.pprint(results, sort_dicts=False)
