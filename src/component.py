import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import boto3
import pytz
from kbc.env_handler import KBCEnvHandler

from woskpace_client import SnowflakeClient

# configuration variables
# aws params
KEY_AWS_PARAMS = 'aws_parameters'
KEY_AWS_API_KEY_ID = 'api_key_id'
KEY_AWS_API_KEY_SECRET = '#api_key_secret'
KEY_AWS_REGION = 'aws_region'
KEY_AWS_S3_BUCKET = 's3_bucket'

KEY_LOADING_OPTIONS = 'loading_options'
KEY_LOADING_OPTIONS_PKEY = 'pkey'
KEY_LOADING_OPTIONS_INCREMENTAL_OUTPUT = 'incremental_output'

KEY_MIN_DATE = 'min_date_since'
KEY_MAX_DATE = 'max_date'
KEY_SINCE_LAST = 'since_last'

KEY_REPORT_PATH_PREFIX = 'report_path_prefix'

# #### Keep for debug
KEY_DEBUG = 'debug'

# list of mandatory parameters => if some is missing, component will fail with readable message on initialization.
MANDATORY_PARS = [KEY_AWS_PARAMS, KEY_REPORT_PATH_PREFIX]
MANDATORY_IMAGE_PARS = []


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
        else:
            logging.getLogger('snowflake.connector').setLevel(logging.WARNING)  # avoid detail logs from the library
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

        self.s3_client = boto3.client('s3',
                                      region_name=self.cfg_params[KEY_AWS_PARAMS][KEY_AWS_REGION],
                                      aws_access_key_id=self.cfg_params[KEY_AWS_PARAMS][KEY_AWS_API_KEY_ID],
                                      aws_secret_access_key=self.cfg_params[KEY_AWS_PARAMS][KEY_AWS_API_KEY_SECRET])

        snfk_authorisation = self.configuration.get_authorization()['workspace']
        params = self.cfg_params  # noqa
        snfwlk_credentials = {
            "account": snfk_authorisation['host'].replace('.snowflakecomputing.com', ''),
            "user": snfk_authorisation['user'],
            "password": snfk_authorisation['password'],
            "database": snfk_authorisation['database'],
            "schema": snfk_authorisation['schema'],
            "warehouse": snfk_authorisation['warehouse']
        }

        self.snowflake_client = SnowflakeClient(**snfwlk_credentials)

        # last state
        self.last_state = self.get_state_file()
        self.last_report_id = self.last_state.get('last_report_id')
        self.last_header = self.last_state.get('report_header', [])

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa

        # last state
        since = params.get(KEY_MIN_DATE) if params.get(KEY_MIN_DATE) else '2000-01-01'
        until = params.get(KEY_MAX_DATE) if params.get(KEY_MAX_DATE) else 'now'
        start_date, end_date = self.get_date_period_converted(since, until)

        until_timestamp = pytz.utc.localize(end_date)

        last_file_timestamp = self.last_state.get('last_file_timestamp')
        if last_file_timestamp and params.get(KEY_SINCE_LAST):
            last_file_timestamp = datetime.fromisoformat(last_file_timestamp)
        else:
            last_file_timestamp = start_date
            last_file_timestamp = pytz.utc.localize(last_file_timestamp)

        report_name = self.report_prefix.split('/')[-1].replace('*', '')

        latest_timestamp = last_file_timestamp
        latest_report_id = self.last_report_id

        logging.info(f"Collecting recent files for report '{report_name}', since {last_file_timestamp}")

        all_files = self._get_s3_objects(self.bucket, self.report_prefix, last_file_timestamp)
        manifests = self._retrieve_report_manifests(all_files, report_name)

        if not params.get(KEY_SINCE_LAST):
            # get only report in specified period
            manifests = [m for m in manifests if
                         datetime.strftime(until_timestamp, '%Y%m%d') >=
                         m['period'].split('-')[0] >= datetime.strftime(last_file_timestamp, '%Y%m%d')
                         ]

        # prep the output
        output_table = os.path.join(self.tables_out_path, report_name)

        # download report files

        logging.info(f"{len(manifests)} recent reports found. Downloading...")

        # get max header
        max_header = self._get_max_header_normalized(manifests)
        # create result table
        self.snowflake_client.open_connection()
        self._create_result_table(report_name, max_header)

        for man in manifests:
            # just in case
            if man['last_modified'] < last_file_timestamp or man['assemblyId'] == self.last_report_id:
                logging.warning(f"Report ID {man['assemblyId']} already downloaded, skipping.")
                continue

            if last_file_timestamp < man['last_modified']:
                latest_timestamp = man['last_modified']
                latest_report_id = man['assemblyId']

            self._upload_report_chunks_to_workspace(man, report_name)

        # finalize
        self.snowflake_client.close()

        self._write_table_manifest(output_table)
        self.write_state_file({"last_file_timestamp": latest_timestamp.isoformat(),
                               "last_report_id": latest_report_id,
                               "report_header": self.last_header})

        logging.info(f"Extraction finished at {datetime.now().isoformat()}.")

    def _write_table_manifest(self, output_table):
        loading_options = self.cfg_params.get(KEY_LOADING_OPTIONS, {})
        incremental = bool(loading_options.get(KEY_LOADING_OPTIONS_INCREMENTAL_OUTPUT, False))
        pkey = loading_options.get(KEY_LOADING_OPTIONS_PKEY, [])
        self.configuration.write_table_manifest(output_table,
                                                columns=self.last_header,
                                                primary_key=pkey,
                                                incremental=incremental)

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
                manifest['period'] = parent_folder_name
                manifests.append(manifest)
        return manifests

    def _upload_report_chunks_to_workspace(self, manifest, table_name):
        logging.info(
            f"Uploading report ID {manifest['assemblyId']} for period {manifest['period']}"
            f" in {len(manifest['reportKeys'])} report chunks.")
        columns = self._get_manifest_normalized_columns(manifest)
        for key in manifest['reportKeys']:
            # support for // syntax
            key_split = key.split('/')
            if '//' in manifest['report_folder']:
                key = f"{manifest['report_folder']}/{key_split[-2]}/{key_split[-1]}"

            # download
            s3_path = f's3://{self.bucket}/{key}'
            logging.info(f"Uploading chunk {key_split[-1]}")
            self.snowflake_client.copy_csv_into_table_from_s3(table_name,
                                                              columns,
                                                              s3_path,
                                                              self.cfg_params[KEY_AWS_PARAMS][KEY_AWS_API_KEY_ID],
                                                              self.cfg_params[KEY_AWS_PARAMS][KEY_AWS_API_KEY_SECRET])

    def _read_s3_file_contents(self, key):
        response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
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

    def _get_s3_objects(self, bucket, prefix, since=None, until=None):
        if prefix.endswith('*'):
            is_wildcard = True
            prefix = prefix[:-1]
        else:
            is_wildcard = False

        paginator = self.s3_client.get_paginator('list_objects_v2')
        params = dict(Bucket=bucket,
                      Prefix=prefix,
                      PaginationConfig={
                          'MaxItems': 100000,
                          'PageSize': 1000
                      })

        counter = 0
        pages = paginator.paginate(**params)
        for page in pages:

            for obj in page.get('Contents', []):
                key = obj['Key']

                if since and obj['LastModified'] <= since:
                    continue
                if until and obj['LastModified'] > until:
                    continue

                if (is_wildcard and key.startswith(prefix)) or key == prefix:
                    counter += 1
                    yield obj

    def _cleanup_report_prefix(self):
        # clean prefix
        if self.report_prefix.endswith('/'):
            self.report_prefix = self.report_prefix[:-1]

        # prepend / in case the path is not with // syntax
        if not self.report_prefix.startswith('/') and '//' not in self.report_prefix:
            self.report_prefix = '/' + self.report_prefix
        if not self.report_prefix.endswith('*'):
            self.report_prefix = self.report_prefix + '*'

    def _get_max_header_normalized(self, manifests):
        for m in manifests:
            # normalize
            norm_cols = set(self._get_manifest_normalized_columns(m))
            if not norm_cols.issubset(set(self.last_header)):
                norm_cols.update(set(self.last_header))
                self.last_header = list(norm_cols)
                self.last_header.sort()

        return self.last_header

    def _get_manifest_normalized_columns(self, manifest):
        # normalize
        man_cols = [col['category'] + '/' + col['name'] for col in manifest['columns']]
        man_cols = self._kbc_normalize_header(man_cols)
        return self._dedupe_header(man_cols)

    def _kbc_normalize_header(self, header):
        normalized = []

        for h in header:
            new_h = h.replace('/', '__')
            new_h = re.sub("[^a-zA-Z\\d_]", "_", new_h)
            normalized.append(new_h)
        return normalized

    def _dedupe_header(self, header, index_separator='_'):
        new_header = list()
        new_header_lower = list()
        dup_cols = dict()
        for c in header:
            if c.lower() in new_header_lower:
                new_index = dup_cols.get(c.lower(), 0) + 1
                new_header.append(c + index_separator + str(new_index))
                dup_cols[c.lower()] = new_index
            else:
                new_header_lower.append(c.lower())
                new_header.append(c)
        return new_header

    # TODO: support for datatypes
    def _create_result_table(self, report_name, max_header):
        columns = []
        for h in max_header:
            columns.append({"name": h, "type": 'TEXT'})
        self.snowflake_client.create_table(report_name, columns)


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
