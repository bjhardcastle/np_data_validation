import datetime
import os
import pathlib
import pickle
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree
from typing import Dict, Generator, List, Union

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

import data_validation as dv

XML_SYMLINK_FOLDER = pathlib.Path(R"\\allen\programs\mindscope\workgroups\dynamicrouting\ben\settings_xml_files")

XML_FILE_FOLDER = pathlib.Path(R"\\allen\programs\mindscope\workgroups\dynamicrouting\ben\settings_xml_files_actual")

XML_LOCATIONS = [
    R'//allen/programs/mindscope',
    R'//allen/programs/braintv',
    R'//allen/programs/aind',
]


def letter_to_idx(letter: str) -> int:
    return ord(letter.upper()) - ord('A')


def idx_to_letter(idx: int) -> str:
    return chr(ord('A') + idx)


class Xml:

    def __init__(self, path: Union[str, pathlib.Path]):
        self.path = path

    def __getstate__(self):
        """Provides transient properties that aren't pickled"""
        state = self.__dict__.copy()
        # Remove entries we don't want to save to disk.
        state['_contents'] = None
        return state

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, path: Union[str, pathlib.Path]):
        self._path = pathlib.Path(path).resolve()

    @property
    def contents(self):
        if not hasattr(self, '_contents') or not self._contents:
            self.contents = xml.etree.ElementTree.parse(self.path)
        return self._contents

    @contents.setter
    def contents(self, value):
        self._contents = value

    @property
    def host(self):
        if not hasattr(self, '_host'):
            result = [host.text for host in self.contents.getroot().iter() if host.tag == 'MACHINE']
            if not result:
                result = [host.attrib.get('machine', None) for host in self.contents.getroot().iter()]
            self._host = result[0]
        return self._host

    @property
    def date(self) -> datetime.date:
        if not hasattr(self, '_date'):
            result = [date.text for date in self.contents.getroot().iter() if date.tag == 'DATE']
            if not result:
                result = [date.attrib.get('date', None) for date in self.contents.getroot().iter()]
            self._date = datetime.datetime.strptime(result[0], '%d %b %Y %H:%M:%S').date()
        return self._date

    @property
    def probe_dicts(self) -> List[dict]:
        if not hasattr(self, '_probe_dicts'):
            self._probe_dicts = [
                probe_dict.attrib
                for probe_dict in self.contents.getroot().iter()
                if 'probe_serial_number' in probe_dict.attrib
            ]
        return self._probe_dicts

    def probe_attrib(self, attrib: str) -> List[str]:
        return [probe.get(attrib, None) for probe in self.probe_dicts]

    @property
    def probes(self) -> List[int]:
        return self.probe_attrib('probe_serial_number')

    @property
    def probe_idx(self) -> List[int]:
        # - normally probe 0-5, corresponding to A-F
        # we could assume each probe in the xml file is 0-5
        # but if, say, we only had one probe in the xml file,
        # it would be labelled as probe 0/A, which might not be true
        # instead, we try to reconstruct index from probe slot and port
        # - normally 2 slots, 3 ports per slot
        slots = self.probe_attrib('slot')
        ports = self.probe_attrib('port')
        return [(int(s) - int(min(slots))) * len(set(ports)) + int(p) - 1 for s, p in zip(slots, ports)]

    @property
    def probe_map(self) -> Dict[str, int]:
        # combine self.xml.probes with sorted probe letters
        return {idx_to_letter(idx): serial for idx, serial in zip(self.probe_idx, self.probes)}


class Recording:
    # has xml file
    # has probes
    #* methods:
    # convert sorted data path into slot/index
    # convert slot/index into serial number from xml file

    class MissingSortedDataError(Exception):
        pass

    class MissingProbeInfoError(xml.etree.ElementTree.ParseError):
        pass

    def __init__(self, path: Union[str, pathlib.Path]):
        self.path = pathlib.Path(path)
        if self.path.name.endswith('.xml'):
            self._xml = Xml(self.path)
        elif self.path.is_dir():
            self._xml = Xml([x for x in self.path.rglob('settings.xml')][0])
        elif self.path.is_file():
            self._xml = Xml([x for x in self.path.parent.rglob('settings.xml')][0])

        self.check()

    def check(self):
        if not self.xml.probes:
            raise Recording.MissingProbeInfoError(f"{self.xml.path} has no probes in xml file")

    def __hash__(self):
        return hash(self.xml.path.as_posix())

    @property
    def path(self) -> pathlib.Path:
        return self._path

    @path.setter
    def path(self, value: Union[str, pathlib.Path]):
        self._path = pathlib.Path(value).resolve()

    @property
    def xml(self) -> Xml:
        return self._xml

    @property
    def rig(self) -> str:
        """map acq hostname to rig"""
        rigs = {dv.nptk.ConfigHTTP.get_np_computers(idx)[f"NP.{idx}-Acq"]: f"NP.{idx}" for idx in range(5)}
        # add old np.2 acq
        rigs['W10DT05517'] = 'NP.2'
        return rigs.get(self.xml.host, 'unknown')


class SortedRecording(Recording):
    # each recording has an xml file. grab that, and extract probe serial numbers
    # then look for sorted data in metrics csv files

    # currently subclasses or calls Session just to verify we have an np-exp
    # since it's easy to find sorted data for these..

    #  spike data for each recording
    #  - snr, amplitude, quality, spread, halfwidth, duration, spread, PT_ratio ?,
    #  date, machine/rig

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.check() # raise an error if sorted data or xml data will prevent us from continuing
                     # we need to have a valid session path to easily find
                     # sorted data: the following will raise a value error if we don't
        self.session = dv.Session(str(self.xml.path))

    def check(self):
        # all sorted probes should have their serial number in the
        # xml file, but not necessarily the other way round
        # check that xml probes match sorted probes
        if not self.metrics_files:
            raise Recording.MissingSortedDataError(f"{self.metrics_root} has no metrics.csv files")
        if not len(self.xml.probe_idx) >= len(self.sorted_probes):
            raise Recording.MissingProbeInfoError(
                f"{self.xml.path.as_uri()} has fewer probes in xml file than metrics.csv files:" +
                f"{self.xml.probes=}, {self.sorted_probes=}")
        if not all([p in self.xml.probe_map.keys() for p in self.sorted_probes]):
            raise Recording.MissingProbeInfoError(
                f'mismatched probe info: serial_numbers={self.xml.probe_map.keys()}, {self.sorted_probes=}')

    def __getstate__(self):
        """Provides transient properties that aren't pickled"""
        state = self.__dict__.copy()
        # Remove entries we don't want to save to disk.
        state['_metrics_dfs'] = None
        return state

    @property
    def metrics_root(self) -> pathlib.Path:
        return pathlib.Path(dv.SessionFile(self.xml.path).session_folder_path)

    def metrics_paths(self) -> Generator[pathlib.Path, None, None]:
        # currently, this doesn't try very hard:
        # if the path has an np-exp session str in it, we know where to look for sorted data:
        for metrics in self.metrics_root.rglob('metrics.csv'):
            yield metrics

    @property
    def metrics_files(self) -> List[pathlib.Path]:
        if not hasattr(self, '_metrics_files'):
            self._metrics_files = []
            for metrics in self.metrics_paths():
                self._metrics_files.append(metrics)
        skip_list = ['cortical_sort']
        return [m for m in self._metrics_files if not any(s in str(m) for s in skip_list)]

    @property
    def sorted_probes(self) -> List[str]:
        if not hasattr(self, '_sorted_probes'):
            mp = []
            for m in self.metrics_files:
                mp.append(re.findall('probe([A-Z])', str(m))[0])
            self._sorted_probes = mp
        return self._sorted_probes

    @property
    def probe_letter_to_serial_number_map(self) -> Dict[str, str]:
        return {probe_letter: self.xml.probe_map[probe_letter] for probe_letter in self.sorted_probes}

    @property
    def probe_serial_to_metrics_map(self) -> Dict[str, pathlib.Path]:
        return {p: m for p, m in zip([self.xml.probe_map[probe] for probe in self.sorted_probes], self.metrics_files)}

    """
    # this method will save all pd.dfs to disk: ~ 600MB for all probes
    # revert back to this when finished with classes and moving on to plotting
    @property
    def metrics_dfs(self) -> Dict[str,pd.DataFrame]:
        if not hasattr(self,'_metrics_dfs'):
            self._metrics_dfs = {k: pd.read_csv(v) for k, v in self.probe_serial_to_metrics_map}
        return self._metrics_dfs
    """

    @property
    def metrics_dfs(self) -> Dict[str, pd.DataFrame]:
        """Transient property"""
        if not hasattr(self, '_metrics_dfs') or not self.metrics_dfs:
            self.metrics_dfs = {k: pd.read_csv(v) for k, v in self.probe_serial_to_metrics_map.items()}
        return self._metrics_dfs

    @metrics_dfs.setter
    def metrics_dfs(self, value: Dict[str, pd.DataFrame]):
        self._metrics_dfs = value

    def describe(self, probe_serial: int):
        # as an example of usage
        return self.metrics_dfs()[str(probe_serial)].describe()


class Probe:
    """A probe is specified by its serial number"""

    all_recs: List[Recording] = None
    sorted_recs: List[SortedRecording] = None

    def __init__(self, serial_number=None):
        if not serial_number:
            raise ValueError('serial_number must be specified')
        self._serial_number = int(serial_number)

    def __hash__(self) -> int:
        return hash(self.serial_number)

    def __eq__(self, other) -> bool:
        return self.serial_number == other.serial_number

    @property
    def serial_number(self) -> int:
        # property is read-only
        return self._serial_number

    @property
    def date0(self) -> datetime.datetime:
        return min([rec.xml.date for rec in self.recs])

    @property
    def date1(self) -> datetime.datetime:
        return max([rec.xml.date for rec in probe.recs])

    def add_rec(self, rec: Recording):

        # xml+sorted data lives here
        if isinstance(rec, SortedRecording):
            if not hasattr(self, 'sorted_recs') \
                or not self.sorted_recs:
                self.sorted_recs = []
            if str(self.serial_number) in rec.probe_letter_to_serial_number_map.values():
                self.sorted_recs.append(rec)

        # both xml-only and xml+sorted lives here
        if isinstance(rec, Recording):
            if not hasattr(self, 'all_recs') \
                or not self.all_recs:
                self.all_recs = []
            self.all_recs.append(rec)

    # has recordings/session files
    @property
    def recs(self) -> List[Recording]:
        # shorthand for sorted_recs:
        # request all_recs explicitly
        return self.sorted_recs
        # no set method

    @property
    def metrics(self) -> Dict[str, pd.DataFrame]:
        if hasattr(self, '_metrics_by_age'):
            return self._metrics_by_age

    def get_metrics_by_age(self) -> Dict[str, pd.DataFrame]:
        if not self.sorted_recs:
            return
        dfs = dict()

        for r in self.recs:
            if not r.metrics_files:
                continue
            d = r.describe(self.serial_number)
            for m in d.keys():
                new = pd.DataFrame(d[m]).transpose()
                new['date'] = r.xml.date
                new['probe_age'] = r.xml.date - self.date0
                if m not in dfs.keys():
                    dfs[m] = pd.DataFrame()
                dfs[m] = dfs[m].append(new)
        if not dfs:
            return
        for d in dfs.values():
            d.set_index('probe_age', inplace=True)
            d.sort_index(inplace=True)

        self._metrics_by_age = dfs
        return dfs


def generate_xml_symlinks():
    """Globs for xml files in specified locations and creates symlinks to them"""

    pathlib.Path(XML_SYMLINK_FOLDER).mkdir(parents=True, exist_ok=True)

    # by default, remote-to-remote symlinks are disabled in Windows
    # enable them here:
    #? is this persistent?
    subprocess.run('fsutil behavior set SymlinkEvaluation R2R:1', check=True, shell=True)

    def hash_path(path):
        return int((hash(pathlib.Path(path).as_posix())**2)**0.5)

    for location in XML_LOCATIONS:
        print(f"checking {location}")

        for root, _, files in os.walk(pathlib.Path(location), followlinks=True):
            for file in files:
                if file == 'settings.xml':

                    target_path = pathlib.Path(root, file)
                    file_root = dv.Session.folder(root)

                    symlink_filename = f"{file_root or hash_path(target_path)}.settings.xml"
                    symlink_path = pathlib.Path(XML_SYMLINK_FOLDER, symlink_filename)

                    try:
                        symlink_path.symlink_to(target_path)
                    except FileExistsError:
                        pass

                    sys.stdout.write(f" symlink created: {symlink_filename}\r")
                    sys.stdout.flush()


def copy_symlinks_to_files():
    for file in XML_SYMLINK_FOLDER.glob("*.settings.xml"):

        # copy file to file repo
        dest = XML_FILE_FOLDER / file.name

        if dest.exists():
            continue

        try:
            result = shutil.copy2(str(file), str(dest), follow_symlinks=True)
        except FileNotFoundError:
            print(f"{file} not found")


def load_probes():
    probes_file = 'probe_data.pkl'

    try:
        with open(probes_file, 'rb') as f:
            probes = pickle.load(f)

    except FileNotFoundError:

        probes = []

        for file in XML_SYMLINK_FOLDER.glob('*.settings.xml'):

            try:
                rec = SortedRecording(file)
            except (Recording.MissingProbeInfoError, FileNotFoundError) as e:
                # no probe data, or some other critical error
                continue
            except (ValueError):
                # not a session file (required for now)
                continue
            except (Recording.MissingSortedDataError) as e:
                # maybe no sorted data or not a prod ephys session
                print(e)
                # but we can still use the datapoint for the rec
                try:
                    # if this fails for any reason we'll just skip it
                    rec = Recording(file)
                except:
                    continue

            for serial in rec.xml.probes:

                p = Probe(serial)

                if p not in probes:
                    probes.append(p)

                # add same rec to multiple probes
                idx = probes.index(p)
                probes[idx].add_rec(rec)

                print(len(probes[idx].all_recs))

        for probe in probes:
            try:
                x = probe.get_metrics_by_age()
            except Exception as e:
                print(e)

        with open(probes_file, 'wb') as f:
            pickle.dump(probes, f)
    return probes


if __name__ == "__main__":
    probes = load_probes()

    metric = 'amplitude'
    for probe in [p for p in probes if p.metrics]:
        plt.subplot(1, 2, 1)
        sns.lineplot(x=probe.metrics[metric].index, y=probe.metrics[metric]['mean'])
        plt.subplot(1, 2, 2)
        sns.lineplot(x=probe.metrics[metric].index,
                     y=(probe.metrics[metric]['mean'] - probe.metrics[metric]['mean'][0]))
    plt.show()
