import datetime
import pathlib
import re
import shutil
import xml.etree.ElementTree as ET
from typing import Generator, List, Set, Union

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
            self.content = xmltodict.parse(xml,
                                           xml_attribs = True,
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
            self.contents = ET.parse(self.path)
        return self._contents

    @contents.setter
    def contents(self, value):
        self._contents = value

    @property
    def host(self):
        return self.contents.findall('INFO')[0].findall('MACHINE')[0].text

    @property
    def date(self) -> datetime.date:
        date = self.contents.findall('INFO')[0].findall('DATE')[0].text
        return datetime.datetime.strptime(date, '%d %b %Y %H:%M:%S').date()

    @property
    def probes(self) -> List[int]:
        if not hasattr(self, '_probes'):
            self.probes = re.findall(R"probe_serial_number': '(\d+)'", str(self.content))
        return self._probes
    
    @probes.setter
    def probes(self, value):
        self._probes = value
        
    @property
    def probe_dict_letters(self) -> str:
        pass
    # TODO might not be necessary to work out from slot+port
    # for probe in self.probe_dicts:
    # return self.probe_dicts[0]['probe_letter']

    @property
    def probe_dicts(self) -> List[dict]:
        return self.content['SETTINGS']['SIGNALCHAIN'][0]['PROCESSOR'][0]['EDITOR']['PROBE']

class Recording(dv.Session):
    # isa Session
    # has xml file
    # has probes
    #* methods:
    # convert path into slot/index
    # convert slot/index into serial number from xml file

    def __init__(self, path: Union[str, pathlib.Path]):
        self._path = pathlib.Path(path)
        self._xml = Xml(self.path)
        super().__init__(self.path)

    @property
    def path(self):
        return self._path.resolve()

    @property
    def xml(self):
        return self._xml

    @property
    def probe_map(self) -> dict:
        # combine self.xml.probes with sorted probe letters
        pass

    @property
    def metrics(self):
        if not hasattr(self, '_metrics'):
            self._metrics = []
            for metrics in pathlib.Path(self.xml.session_folder_path).rglob('metrics.csv'):
                self._metrics.append(metrics)
        skip_list = ['cortical_sort']
        return [m for m in self._metrics if not any(s in str(m) for s in skip_list)]

    @property
    def metrics_probe_letter(self) -> List[str]:
        mp = []
        for m in self.metrics:
            mp.append(re.findall('probe([A-Z])', str(m))[0])
        return mp

    # rglob for metrics.csv files ... it's reliable, if not fast
    # session = dv.SessionFile(file.resolve().as_posix()).session_folder_path
    # for metrics in pathlib.Path(session).rglob('metrics.csv'):
    #     df = pd.read_csv(metrics)

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

    # has recordings/session files
    @property
    def recs(self) -> Set[Recording]:
        if hasattr(self, '_recs'):
            return self._recs

    @recs.setter
    def recs(self, rec: Union[str, pathlib.Path, Recording, List[Recording]]):
        if hasattr(self, '_recs'):
            self._recs = set()
            if not isinstance(rec, List):
                rec = [rec]
            self._recs.add(pathlib.Path(rec))
        else:
            self._recs = set(rec)

def probe_generator(listed: List[dict]) -> Generator[Probe, None, None]:
    if isinstance(listed, dict):
        yield listed.get('probe_serial_number', None)
    for item in listed:
        probe_generator(item)

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

    probes = set()
    tf=[]
    for file in XML_SYMLINK_FOLDER.glob('*.settings.xml'):
        try:
            rec = Recording(file)
        except ValueError:
            continue
        # rec.metrics_probe_letter
        # print(rec.xml.probes)
        tf += [len(rec.xml.probes) == len(rec.metrics)]
