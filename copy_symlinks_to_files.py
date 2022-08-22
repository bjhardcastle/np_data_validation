import datetime
import pathlib
import pickle
import re
import shutil
import xml.etree.ElementTree
from typing import Dict, List, Union

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import xmltodict

import data_validation as dv

XML_SYMLINK_FOLDER = pathlib.Path(R"\\allen\programs\mindscope\workgroups\dynamicrouting\ben\settings_xml_files")

XML_FILE_FOLDER = pathlib.Path(R"\\allen\programs\mindscope\workgroups\dynamicrouting\ben\settings_xml_files_actual")
# for file in os.path.glob(R"\\allen\programs\mindscope\workgroups\dynamicrouting\ben\settings_xml_files\*"):


class Xml(dv.SessionFile):

    def __init__(self, path: Union[str, pathlib.Path]):
        self.path = path
        super().__init__(self.path)

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, path: Union[str, pathlib.Path]):
        self._path = pathlib.Path(path).resolve()

    @property
    def content(self):
        if not hasattr(self, '_content'):
            with open(self.path, 'r', encoding='utf-8') as file:
                xml = file.read()
            self.content = xmltodict.parse(
                xml,
                xml_attribs=True,
                encoding='utf-8',
                process_namespaces=False,
                namespace_separator=':',
                attr_prefix='',
                                            #    item_depth=0,
                force_list=True,
                cdata_key='#text')
        return self._content

    @content.setter
    def content(self, value):
        self._content = value

    @property
    def contents(self):
        if not hasattr(self, '_contents'):
            self.contents = xml.etree.ElementTree.parse(self.path)
        return self._contents

    @contents.setter
    def contents(self, value):
        self._contents = value

    @property
    def host(self):
        result = [host.text for host in self.contents.getroot().iter() if host.tag == 'MACHINE']
        if not result:
            result = [host.attrib.get('machine', None) for host in self.contents.getroot().iter()]
        return result[0]

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
        return {Probe.idx_to_letter(idx): serial for idx, serial in zip(self.probe_idx, self.probes)}


class Recording(dv.Session):
    # isa Session
    # has xml file
    # has probes
    #* methods:
    # convert path into slot/index
    # convert slot/index into serial number from xml file
    class MissingSortedDataError(FileNotFoundError):
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

        super().__init__(self.path)

        self.check() # raise an error if sorted data or xml data will prevent us from continuing

    def check(self):
        # all sorted probes should have their serial number in the
        # xml file, but not necessarily the other way round
        # check that xml probes match sorted probes
        if not self.metrics:
            raise Recording.MissingSortedDataError(f"{self.xml.session_folder_path} has no metrics.csv files")
        if not self.xml.probes:
            raise Recording.MissingProbeInfoError(f"{self.xml.path} has no probes in xml file")
        if not len(self.xml.probe_idx) >= len(self.sorted_probes):
            raise Recording.MissingProbeInfoError(
                f"{self.xml.path.as_uri()} has fewer probes in xml file than metrics.csv files:" +
                f"{self.xml.probes=}, {self.sorted_probes=}")
        if not all([p in self.xml.probe_map.keys() for p in self.sorted_probes]):
            raise Recording.MissingProbeInfoError(
                f'mismatched probe info: serial_numbers={self.xml.probe_map.keys()}, {self.sorted_probes=}')

    def __hash__(self):
        return hash(self.xml.path.as_posix())

    @property
    def path(self) -> pathlib.Path:
        return self._path.resolve()

    @path.setter
    def path(self, value: Union[str, pathlib.Path]):
        self._path = pathlib.Path(value)

    @property
    def xml(self):
        return self._xml

    @property
    def metrics(self) -> List[pathlib.Path]:
        if not hasattr(self, '_metrics'):
            self._metrics = []
            for metrics in pathlib.Path(self.xml.session_folder_path).rglob('metrics.csv'):
                self._metrics.append(metrics)
        skip_list = ['cortical_sort']
        return [m for m in self._metrics if not any(s in str(m) for s in skip_list)]

    @property
    def sorted_probes(self) -> List[str]:
        if not hasattr(self, '_sorted_probes'):
            mp = []
            for m in self.metrics:
                mp.append(re.findall('probe([A-Z])', str(m))[0])
            self._sorted_probes = mp
        return self._sorted_probes

    @property
    def probe_letter_to_serial_number_map(self) -> Dict[str, str]:
        return {probe_letter: self.xml.probe_map[probe_letter] for probe_letter in self.sorted_probes}

    @property
    def probe_serial_to_metrics_map(self) -> Dict[str, pathlib.Path]:
        if not hasattr(self, '_probe_serial_to_metrics_map'):
            self._probe_serial_to_metrics_map = {
                p: m for p, m in zip([self.xml.probe_map[probe] for probe in self.sorted_probes], self.metrics)
            }
        return self._probe_serial_to_metrics_map

    """
    # this method will save all pd.dfs to disk: ~ 600MB for all probes
    # revert back to this when finished with classes and moving on to plotting
    @property
    def metrics_dfs(self) -> Dict[str,pd.DataFrame]:
        if not hasattr(self,'_metric_dfs'):
            self._metric_dfs = {k: pd.read_csv(v) for k, v in self.probe_serial_to_metrics_map}
        return self._metric_dfs
    """

    def metrics_dfs(self) -> Dict[str, pd.DataFrame]:
        return self._metric_dfs

    def get_metrics_dfs(self):
        if not hasattr(self, '_metric_dfs'):
            self._metric_dfs = {k: pd.read_csv(v) for k, v in self.probe_serial_to_metrics_map.items()}

    def describe(self, probe_serial: int):
        self.get_metrics_dfs()
        # as an example of usage
        return self.metrics_dfs()[str(probe_serial)].describe()

    #  spike data for each recording
    #  - snr, amplitude, quality, spread, halfwidth, duration, spread, PT_ratio ?,
    #  date, machine/rig

    # each recording session has a n xml file. grab that, and extract probe serial numbers

    # then look for sorted data in metrics csv files


class Probe:
    """A probe is specified by its serial number"""

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
    
    # has recordings/session files
    @property
    def recs(self) -> List[Recording]:
        if hasattr(self, '_recs'):
            return self._recs
        else:
            return None

    @recs.setter
    def recs(self, rec: Union[str, pathlib.Path, Recording]):
        if isinstance(rec, (str, pathlib.Path)):
            rec = Recording(rec)
        if not hasattr(self, '_recs'):
            self._recs = []
        if rec in self._recs:
            return
        self._recs.append(rec)


    @staticmethod
    def letter_to_idx(letter: str) -> int:
        return ord(letter.upper()) - ord('A')

    @staticmethod
    def idx_to_letter(idx: int) -> str:
        return chr(ord('A') + idx)
    
    @property
    def metrics(self) -> Dict[str, pd.DataFrame]:
        if hasattr(self,'_metrics_by_age'):
            return self._metrics_by_age

    def get_metrics_by_age(self) -> Dict[str, pd.DataFrame]:
        dfs = dict()

        for r in self.recs:
            d = r.describe(self.serial_number)
            for m in d.keys():
                new = pd.DataFrame(d[m]).transpose()
                new['date'] = r.xml.date
                new['probe_age'] = r.xml.date - self.date0
                if m not in dfs.keys():
                    dfs[m] = pd.DataFrame()
                dfs[m] = dfs[m].append(new)

        for d in dfs.values():
            d.set_index('probe_age', inplace=True)
            d.sort_index(inplace=True)

        self._metrics_by_age = dfs
        return dfs


def copy_symlinks_to_files():
    for file in XML_SYMLINK_FOLDER.glob("*.settings.xml"):

        # copy file to file_repo
        dest = XML_FILE_FOLDER / file.name

        if dest.exists():
            continue

        try:
            result = shutil.copy2(str(file), str(dest), follow_symlinks=True)
            print(result)
        except FileNotFoundError:
            print(f"{file} not found")


if __name__ == "__main__":
    probes_file = 'probe_data.pkl'

    try:
        with open(probes_file, 'rb') as f:
            probes = pickle.load(f)

    except FileNotFoundError:

        probes = []

        for file in XML_SYMLINK_FOLDER.glob('*.settings.xml'):

            try:
                rec = Recording(file)
            except (Recording.MissingSortedDataError, Recording.MissingProbeInfoError) as e:
                print(e)
            except (ValueError, FileNotFoundError):
                continue

            for serial in rec.xml.probes:

                p = Probe(serial)

                if p not in probes:
                    probes.append(p)
                idx = probes.index(p)
                if str(probes[idx].serial_number) in rec.probe_letter_to_serial_number_map.values():
                    probes[idx].recs = rec # add to recs list
                    print(len(probes[idx].recs))

        for probe in probes:
            x = probe.get_metrics_by_age()

        with open(probes_file, 'wb') as f:
            pickle.dump(probes, f)
            

    metric = 'amplitude'
    for probe in probes:
        plt.subplot(1,2,1)
        sns.lineplot(x=probe.metrics[metric].index,
                     y=probe.metrics[metric]['mean'])
        plt.subplot(1,2,2)
        sns.lineplot(x=probe.metrics[metric].index,
                     y=(probe.metrics[metric]['mean']-probe.metrics[metric]['mean'][0]))
    plt.show()
    
    
