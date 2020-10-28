'''
Template Component main class.

'''

import csv
import gzip
import json
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import boto3
import pytz
from kbc.csv_tools import CachedOrthogonalDictWriter
from kbc.env_handler import KBCEnvHandler

# configuration variables
# aws params
KEY_AWS_PARAMS = 'aws_parameters'
KEY_AWS_API_KEY_ID = 'api_key_id'
KEY_AWS_API_KEY_SECRET = '#api_key_secret'
KEY_AWS_REGION = 'aws_region'
KEY_AWS_S3_BUCKET = 's3_bucket'

KEY_REPORT_PATH_PREFIX = 'report_path_prefix'

# #### Keep for debug
KEY_DEBUG = 'debug'

# list of mandatory parameters => if some is missing, component will fail with readable message on initialization.
MANDATORY_PARS = []
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.0.1'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        # for easier local project setup
        default_data_dir = Path(__file__).resolve().parent.parent.joinpath('data').as_posix() \
            if not os.environ.get('KBC_DATADIR') else None

        KBCEnvHandler.__init__(self, MANDATORY_PARS, log_level=logging.DEBUG if debug else logging.INFO,
                               data_path=default_data_dir)
        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            # validation of mandatory parameters. Produces ValueError
            self.validate_config(MANDATORY_PARS)
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)
        self.bucket = self.cfg_params[KEY_AWS_PARAMS][KEY_AWS_S3_BUCKET]
        self.report_prefix = self.cfg_params[KEY_REPORT_PATH_PREFIX]
        self._cleanup_report_prefix()

        self.client = boto3.client('s3',
                                   region_name=self.cfg_params[KEY_AWS_PARAMS][KEY_AWS_REGION],
                                   aws_access_key_id=self.cfg_params[KEY_AWS_PARAMS][KEY_AWS_API_KEY_ID],
                                   aws_secret_access_key=self.cfg_params[KEY_AWS_PARAMS][KEY_AWS_API_KEY_SECRET])

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa

        # last state
        last_state = self.get_state_file()

        last_file_timestamp = last_state.get('last_file_timestamp')
        if last_file_timestamp:
            last_file_timestamp = datetime.fromisoformat(last_file_timestamp)
        else:
            last_file_timestamp = datetime(2000, 1, 1)
            last_file_timestamp = pytz.utc.localize(last_file_timestamp)

        last_report_id = last_state.get('last_report_id')
        current_header = last_state.get('report_header', [])

        report_name = self.report_prefix.split('/')[-1].replace('*', '')

        logging.info(f"Collecting recent files for report '{report_name}', since {last_file_timestamp}")

        all_files = self._get_s3_objects(self.bucket, self.report_prefix, last_file_timestamp)
        manifests = self._retrieve_report_manifests(all_files, report_name)

        # prep the output
        output_file = os.path.join(self.tables_out_path, report_name)
        tmp_path = os.path.join(self.data_path, 'tmp')
        os.makedirs(tmp_path, exist_ok=True)

        # download report files

        # init writer
        writer = CachedOrthogonalDictWriter(output_file, current_header)

        latest_timestamp = last_file_timestamp
        latest_report_id = last_report_id
        logging.info(f"{len(manifests)} recent reports found. Downloading...")

        for man in manifests:
            # just in case
            if man['last_modified'] < last_file_timestamp or man['assemblyId'] == last_report_id:
                continue

            if last_file_timestamp < man['last_modified']:
                latest_timestamp = man['last_modified']
                latest_report_id = man['assemblyId']
            downloaded_chunks = self._download_report_chunks(man, tmp_path)

            logging.info("Normalizing headers, writing results.")
            result_files = self._process_chunks(downloaded_chunks)
            self._normalize_headers_write(result_files, writer)

        # finalize

        result_header = writer.fieldnames
        writer.close()
        self.configuration.write_table_manifest(output_file, columns=result_header)
        self.write_state_file({"last_file_timestamp": latest_timestamp.isoformat(),
                               "last_report_id": latest_report_id,
                               "report_header": result_header})

        logging.info("Extraction finished.")

    def _retrieve_report_manifests(self, all_files, report_name):
        manifests = []
        for obj in all_files:
            object_name = obj['Key'].split('/')[-1]
            parent_folder_name = obj['Key'].split('/')[-2]
            start_date, end_date = self._try_to_parse_report_period(parent_folder_name)
            # get only root (period) manifests
            manifest_file_name = f"{report_name}-Manifest.json"
            if start_date and object_name == manifest_file_name:
                # download file content
                manifest = json.loads(self._read_s3_file_contents(obj['Key']))
                manifest['last_modified'] = obj['LastModified']
                manifest['report_folder'] = obj['Key'].replace(f'/{manifest_file_name}', '')
                manifests.append(manifest)
        return manifests

    def _download_report_chunks(self, manifest, output_folder):
        result_files = []
        for key in manifest['reportKeys']:
            chunk_file_name = key.replace('/', '_')
            result_file_path = os.path.join(output_folder, chunk_file_name)
            # download
            self.client.download_file(Bucket=self.bucket,
                                      Key=key,
                                      Filename=result_file_path)
            result_files.append(result_file_path)
        return result_files

    def _read_s3_file_contents(self, key):
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response['Body'].read()

    def _try_to_parse_report_period(self, folder_name):
        periods = folder_name.split('-')
        start_date = None
        end_date = None
        if len(periods) == 2:
            try:
                start_date = datetime.strptime(periods[0], '%Y%m%d')
                end_date = datetime.strptime(periods[1], '%Y%m%d')
            except Exception:
                pass

        return start_date, end_date

    def _get_s3_objects(self, bucket, prefix, since=None):
        if prefix.endswith('*'):
            is_wildcard = True
            prefix = prefix[:-1]
        else:
            is_wildcard = False

        paginator = self.client.get_paginator('list_objects_v2')
        params = dict(Bucket=bucket,
                      Prefix=prefix,
                      PaginationConfig={
                          'MaxItems': 100000,
                          'PageSize': 1000
                      })
        has_more = True
        counter = 0
        pages = paginator.paginate(**params)
        for page in pages:
            keys = {}
            for obj in page.get('Contents', []):
                key = obj['Key']

                if since and obj['LastModified'] <= since:
                    continue

                if (is_wildcard and key.startswith(prefix)) or key == prefix:
                    counter += 1
                    yield obj

    def _cleanup_report_prefix(self):
        # clean prefix
        if self.report_prefix.endswith('/'):
            self.report_prefix = self.report_prefix[:-1]

        if not self.report_prefix.startswith('/'):
            self.report_prefix = '/' + self.report_prefix
        if not self.report_prefix.endswith('*'):
            self.report_prefix = self.report_prefix + '*'

    def _process_chunks(self, downloaded_chunks):
        extracted_files = []
        for chunk in downloaded_chunks:
            # extract
            extracted_file = chunk.replace('.gz', '')
            with gzip.open(chunk, 'rb') as f_in:
                with open(chunk.replace('.gz', ''), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            # delete gzip
            os.remove(chunk)
            extracted_files.append(extracted_file)
        return extracted_files

    def _normalize_headers_write(self, result_files, writer):
        # normalize headers
        for res_file in result_files:
            with open(res_file) as in_file:
                reader = csv.DictReader(in_file)
                for row in reader:
                    writer.writerow(row)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug_arg = sys.argv[1]
    else:
        debug_arg = False
    try:
        comp = Component(debug_arg)
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(1)
