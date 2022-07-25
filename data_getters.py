# -*- coding: utf-8 -*-
"""
Created on Tue Jun  9 14:33:49 2020

@author: svc_ccg
"""

import glob
import json
import os  # , shutil

from psycopg2 import connect, extras


class data_getter():
    ''' parent class for data getter, should be able to 
    1) connect to data source
    2) grab experiment data
    3) grab probe data
    '''

    def __init__(self, exp_id=None, base_dir=None, cortical_sort=False):

        self.data_dict = {}
        self.cortical_sort = cortical_sort
        self.connect(exp_id, base_dir)
        self.get_exp_data()
        self.get_probe_data()
        self.get_image_data()

    def connect(self):
        pass

    def get_exp_data(self):
        pass

    def get_probe_data(self):
        pass

    def get_image_data(self):
        pass


class lims_data_getter(data_getter):

    def connect(self, exp_id, base_dir):
        #set up connection to lims
        self.con = connect(
            dbname='lims2',
            user='limsreader',
            host='limsdb2',
            password='limsro',
            port=5432,
        )
        self.con.set_session(
            readonly=True,
            autocommit=True,
        )
        self.cursor = self.con.cursor(cursor_factory=extras.RealDictCursor,)

        self.lims_id = exp_id

    def get_exp_data(self):
        ''' Get all the experiment files
            eg sync, pkls, videos etc
        '''
        WKF_QRY = '''
            SELECT es.id AS es_id, 
                es.name AS es,
                es.storage_directory,
                es.workflow_state,
                es.date_of_acquisition,
                es.stimulus_name,
                es.foraging_id as foraging_id,
                sp.external_specimen_name,
                isi.id AS isi_experiment_id,
                e.name AS rig,
                u.login AS operator,
                p.code AS project,
                wkft.name AS wkft, 
                wkf.storage_directory || wkf.filename AS wkf_path,
                bs.storage_directory AS behavior_dir
            FROM ecephys_sessions es
                JOIN specimens sp ON sp.id = es.specimen_id
                LEFT JOIN isi_experiments isi ON isi.id = es.isi_experiment_id
                LEFT JOIN equipment e ON e.id = es.equipment_id
                LEFT JOIN users u ON u.id = es.operator_id
                JOIN projects p ON p.id = es.project_id
                LEFT JOIN well_known_files wkf ON wkf.attachable_id = es.id
                LEFT JOIN well_known_file_types wkft ON wkft.id=wkf.well_known_file_type_id
                LEFT JOIN behavior_sessions bs ON bs.foraging_id = es.foraging_id
            WHERE es.id = {}
            ORDER BY es.id
            '''
        #

        self.cursor.execute(WKF_QRY.format(self.lims_id))
        exp_data = self.cursor.fetchall()
        self.data_dict.update(exp_data[0])                        #update data_dict to have all the experiment metadata
        [self.data_dict.pop(key) for key in ['wkft', 'wkf_path']] #...but remove the wkf stuff

        for e in exp_data:
            wkft = e['wkft']
            wkf_path = e['wkf_path']
            self.data_dict[wkft] = convert_lims_path(wkf_path)

        self.translate_wkf_names()

        behavior_dir = convert_lims_path(self.data_dict['behavior_dir'])
        self.data_dict['behavior_pkl'] = glob_file(os.path.join(behavior_dir, '*.pkl'))
        if self.data_dict['date_of_acquisition'] is not None:
            self.data_dict['datestring'] = self.data_dict['date_of_acquisition'].strftime('%Y%m%d')
        else:
            self.data_dict['datestring'] = ''
        self.data_dict['es_id'] = str(self.data_dict['es_id'])

    def get_image_data(self):
        '''Get all the images associated with this experiment 
        '''

        IMAGE_QRY = '''
            SELECT es.id AS es_id, es.name AS es, imt.name AS image_type, es.storage_directory || im.jp2 AS image_path
            FROM ecephys_sessions es
                JOIN observatory_associated_data oad ON oad.observatory_record_id = es.id AND oad.observatory_record_type = 'EcephysSession'
                JOIN images im ON im.id=oad.observatory_file_id AND oad.observatory_file_type = 'Image'
                JOIN image_types imt ON imt.id=im.image_type_id
            WHERE es.id = {}
            ORDER BY es.id, imt.name;
            '''

        self.cursor.execute(IMAGE_QRY.format(self.lims_id))
        image_data = self.cursor.fetchall()

        # FOR NOW JUST ASSUME IMAGES ARE IN THE D1 UPLOAD DIRECTORY
        # get D1 directory (assume this is where the sync file is)

        for im in image_data:
            name = im['image_type']
            path = convert_lims_path(im['image_path'])
            #self.data_dict[name] = convert_lims_path(path)
            self.data_dict[name] = path

    def get_probe_data(self):
        ''' Get sorted ephys data for each probe 
        
        TODO: make this actually use the well known file types,
        rather than just grabbing the base directories
        
        '''

        WKF_PROBE_QRY = '''
            SELECT es.id AS es_id, 
                es.name AS es, 
                ep.name AS ep, 
                ep.id AS ep_id, 
                wkft.name AS wkft, 
                wkf.storage_directory || wkf.filename AS wkf_path
            FROM ecephys_sessions es
                JOIN ecephys_probes ep ON ep.ecephys_session_id=es.id
                LEFT JOIN well_known_files wkf ON wkf.attachable_id = ep.id
                LEFT JOIN well_known_file_types wkft ON wkft.id=wkf.well_known_file_type_id
            WHERE es.id = {} 
            ORDER BY es.id, ep.name;
            '''
        self.cursor.execute(WKF_PROBE_QRY.format(self.lims_id))
        probe_data = self.cursor.fetchall()

        p_info = [p for p in probe_data if p['wkft'] == 'EcephysSortedAmplitudes']

        getnesteddir = lambda x: os.path.dirname(os.path.dirname(os.path.dirname(x)))
        probe_bases = [convert_lims_path(getnesteddir(pi['wkf_path'])) for pi in p_info]
        #probe_bases = [convert_lims_path(os.path.dirname(pi['wkf_path'])) for pi in p_info]

        self.data_dict['data_probes'] = []
        for pb in probe_bases:
            probeID = pb[-1]
            self.data_dict['data_probes'].append(probeID)
            self.data_dict['probe' + probeID] = pb
            self.data_dict['lfp' + probeID] = pb
            info_json = glob_file(os.path.join(pb, '*probe_info*json'))
            self.data_dict['probe' + probeID + '_info'] = info_json

        raw = [p for p in probe_data if p['wkft'] == 'EcephysProbeRawData']
        name_suffix = {
            'probeA': 'ABC',
            'probeB': 'ABC',
            'probeC': 'ABC',
            'probeD': 'DEF',
            'probeE': 'DEF',
            'probeF': 'DEF'
        }
        for r in raw:
            probeID = r['ep']
            name = r['wkft'] + name_suffix[probeID]
            path = convert_lims_path(r['wkf_path'])

            if not name + '_settings' in self.data_dict or self.data_dict[name + '_settings'] is None:
                self.data_dict[name + '_settings'] = path

            npx2_path = glob_file(os.path.join(os.path.dirname(path), '*npx2'))
            if not name in self.data_dict or self.data_dict[name] is None:
                self.data_dict[name] = npx2_path

        self.probe_data = probe_data

    def translate_wkf_names(self):
        wkf_dict = {
            'MappingPickle': 'mapping_pkl',
            'EcephysReplayStimulus': 'replay_pkl',
            'EcephysRigSync': 'sync_file',
            'OptoPickle': 'opto_pkl'
        }

        for wkf in wkf_dict:
            if wkf in self.data_dict:
                self.data_dict[wkf_dict[wkf]] = self.data_dict[wkf]


class local_data_getter(data_getter):

    def connect(self, exp_id, base_dir):

        if os.path.exists(base_dir):
            self.base_dir = base_dir
        else:
            print('Invalid base directory: ' + base_dir)

    def get_exp_data(self):
        file_glob_dict = {
            'mapping_pkl': ['*mapping*.pkl', '*stim.pkl'],
            'replay_pkl': '*replay*.pkl',
            'behavior_pkl': '*behavior*.pkl',
            'opto_pkl': '*opto*.pkl',
            'sync_file': '*.sync',
            'RawEyeTrackingVideo': ['*.eye.avi', '*eye.mp4'],
            'RawBehaviorTrackingVideo': ['*behavior.avi', '*behavior.mp4'],
            'RawFaceTrackingVideo': ['*face.avi', '*face.mp4'],
            'RawEyeTrackingVideoMetadata': '*eye.json',
            'RawBehaviorTrackingVideoMetadata': '*behavior.json',
            'RawFaceTrackingVideoMetadata': '*face.json',
            'EcephysPlatformFile': '*platformD1.json',
            'NewstepConfiguration': '*motor-locs.csv'
        }

        for fn in file_glob_dict:
            if isinstance(file_glob_dict[fn], list):
                paths = [glob_file(os.path.join(self.base_dir, f)) for f in file_glob_dict[fn]]
                path = [p for p in paths if not p is None]
                if len(path) > 0:
                    self.data_dict[fn] = path[0]
            else:
                filepath = glob_file(os.path.join(self.base_dir, file_glob_dict[fn]))
                if filepath is not None:
                    self.data_dict[fn] = filepath

        basename = os.path.basename(self.base_dir)
        self.data_dict['es_id'] = basename.split('_')[0]
        self.data_dict['external_specimen_name'] = basename.split('_')[1]
        self.data_dict['datestring'] = basename.split('_')[2]
        self.data_dict['rig'] = self.get_rig_from_platform()

    def get_platform_info(self):

        platform_file = self.data_dict['EcephysPlatformFile']
        with open(platform_file, 'r') as file:
            self.platform_info = json.load(file)

    def get_rig_from_platform(self):

        if not hasattr(self, 'platform_info'):
            self.get_platform_info()

        return self.platform_info['rig_id']

    def get_probe_data(self):
        self.data_dict['data_probes'] = []

        #get probe dirs
        for probeID in 'ABCDEF':
            if self.cortical_sort:
                probe_base = glob_file(os.path.join(self.base_dir, 'cortical*probe' + probeID + '_sorted'))
                lfp_base = glob_file(os.path.join(self.base_dir, '*probe' + probeID + '_sorted'))
            else:
                probe_base = glob_file(os.path.join(self.base_dir, '*probe' + probeID + '_sorted'))
                lfp_base = probe_base

            if probe_base is not None:
                self.data_dict['data_probes'].append(probeID)
                self.data_dict['probe' + probeID] = probe_base

                metrics_file = glob_file(os.path.join(probe_base, r'continuous\Neuropix-PXI-100.0\metrics.csv'))
                self.data_dict['probe' + probeID + '_metrics'] = metrics_file

                info_json = glob_file(os.path.join(probe_base, '*probe_info*json'))
                self.data_dict['probe' + probeID + '_info'] = info_json

                channel_map = glob_file(os.path.join(probe_base, r'continuous\Neuropix-PXI-100.0\channel_map.npy'))
                self.data_dict['probe' + probeID + '_channel_map'] = channel_map

            if lfp_base is not None:
                self.data_dict['lfp' + probeID] = lfp_base

    def get_image_data(self):

        #GET PROBE DEPTH IMAGES
        for probeID in self.data_dict['data_probes']:

            probe_base = self.data_dict['probe' + probeID]
            probe_depth_image = glob_file(os.path.join(probe_base, 'probe_depth*.png'))
            if probe_depth_image is not None:
                self.data_dict['probe_depth_' + probeID] = probe_depth_image

        #GET OTHER IMAGE FILES
        #image_files = [k for k in D1_local if 'image' in k]
        image_files = [
            'EcephysPostExperimentLeft', 'EcephysPostExperimentRight', 'EcephysPostInsertionLeft',
            'EcephysPostInsertionRight', 'EcephysPostStimulusLeft', 'EcephysPostStimulusRight',
            'EcephysPreExperimentLeft', 'EcephysPreExperimentRight', 'EcephysPreInsertionLeft',
            'EcephysPreInsertionRight', 'EcephysInsertionLocationImage', 'EcephysOverlayImage',
            'EcephysBrainSurfaceLeft', 'EcephysBrainSurfaceRight'
        ]

        for im in image_files:
            im_info = D1_local[im]
            im_file = glob_file(os.path.join(self.base_dir, im_info['rel_path']))

            self.data_dict[im] = im_file


def glob_file(file_path):
    f = glob.glob(file_path)
    if len(f) > 0:
        return f[0]
    else:
        return None


def convert_lims_path(path):
    if path is not None:
        new_path = r'\\' + os.path.normpath(path)[1:]
    else:
        new_path = ''

    return new_path
