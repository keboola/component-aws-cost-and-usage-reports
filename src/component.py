import json
import logging
import os
import re
import tempfile
import zipfile
from datetime import datetime

import boto3
import dateparser
import pytz
from botocore.exceptions import ClientError
from keboola.component.base import ComponentBase

from duckdb_client import DuckDBClient

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

# list of mandatory parameters=> if some is missing, component will fail with readable message on initialization.
MANDATORY_PARS = [KEY_AWS_PARAMS, KEY_REPORT_PATH_PREFIX]
MANDATORY_IMAGE_PARS = []

# Trailing '_<number>' appended to a column name to disambiguate case-colliding columns.
DEDUPE_SUFFIX_PATTERN = re.compile(r'^(?P<base>.+)_(?P<index>\d+)$')

# Date format accepted verbatim, without going through dateparser.
ISO_DATE_FORMAT = '%Y-%m-%d'


class Component(ComponentBase):

    def __init__(self):
        super().__init__(required_parameters=MANDATORY_PARS,
                         required_image_parameters=MANDATORY_IMAGE_PARS)
        logging.info('Loading configuration...')

        aws_params = self.configuration.parameters[KEY_AWS_PARAMS]
        self.bucket = aws_params[KEY_AWS_S3_BUCKET]
        self.report_prefix = self.configuration.parameters[KEY_REPORT_PATH_PREFIX]
        self._cleanup_report_prefix()

        self.s3_client = boto3.client('s3',
                                      region_name=aws_params[KEY_AWS_REGION],
                                      aws_access_key_id=aws_params[KEY_AWS_API_KEY_ID],
                                      aws_secret_access_key=aws_params[KEY_AWS_API_KEY_SECRET])

        # Initialize DuckDB client for local processing
        self.duckdb_client = DuckDBClient()

        # last state
        self.last_state = self.get_state_file()
        self.last_report_id = self.last_state.get('last_report_id')
        self.last_header = self.last_state.get('report_header', [])
        # Which output column each case-differing source column owns. Absent from state
        # written by earlier versions, in which case it is recovered from the header.
        self.column_slots = self.last_state.get('report_column_slots', {})
        self.run_collision_bases = set()

    def run(self):
        '''
        Main execution code
        '''
        params = self.configuration.parameters  # noqa

        # last state
        since = params.get(KEY_MIN_DATE) if params.get(
            KEY_MIN_DATE) else '2000-01-01'
        until = params.get(KEY_MAX_DATE) if params.get(KEY_MAX_DATE) else 'now'
        logging.info(f"{since} {until}")
        start_date, end_date = self.get_date_period_converted(since, until)

        until_timestamp = pytz.utc.localize(end_date)

        incremental_fetch = params.get(KEY_SINCE_LAST)

        last_file_timestamp = self.last_state.get('last_file_timestamp')
        if last_file_timestamp and incremental_fetch:
            since_timestamp = datetime.fromisoformat(last_file_timestamp)
        else:
            since_timestamp = pytz.utc.localize(start_date)

        report_name = self.report_prefix.split('/')[-1].replace('*', '')

        latest_report_id = self.last_report_id

        logging.info(
            f"Collecting recent files for report '{report_name}', since {since_timestamp}")

        all_files = self._get_s3_objects(
            self.bucket, self.report_prefix, since_timestamp)
        manifests = self._retrieve_report_manifests(all_files, report_name)

        if not incremental_fetch:
            # get only report in specified period
            manifests = [m for m in manifests if
                         datetime.strftime(until_timestamp, '%Y%m%d') >=
                         m['billingPeriod']['start'].split(
                             'T')[0] >= datetime.strftime(since_timestamp, '%Y%m%d')
                         ]

        # prep the output
        output_table = os.path.join(self.tables_out_path, report_name)

        # download report files
        reports_found = len(manifests)
        if reports_found > 0:
            logging.info(
                f"{len(manifests)} recent reports found. Downloading...")
        else:
            logging.warning(
                "No reports found for the specified period. If there are some available check the prefix setting.")
            self.write_state_file(self.last_state)
            exit(0)

        # get max header
        max_header = self._get_max_header_normalized(manifests)
        # create result table
        self.duckdb_client.open_connection()
        try:
            self._create_result_table(report_name, max_header)

            for man in manifests:
                # just in case
                if incremental_fetch and man['assemblyId'] == self.last_report_id:
                    logging.warning(
                        f"Report ID {man['assemblyId']} already downloaded, skipping.")
                    continue

                if since_timestamp < man['last_modified']:
                    since_timestamp = man['last_modified']
                    latest_report_id = man['assemblyId']

                self._load_report_chunks_to_duckdb(man, report_name)

            # Export to CSV (VIEW approach discovers columns during export)
            output_csv = f"{output_table}.csv"
            self.duckdb_client.export_to_csv(report_name, output_csv, max_header)

            # Update header with final columns from DuckDB (VIEW may have different normalization)
            final_columns = self.duckdb_client.get_final_columns()
            if final_columns:
                self.last_header = final_columns
                logging.info(f"Updated header with {len(final_columns)} columns from DuckDB VIEW")

            self._write_table_manifest(output_table, report_name)
            self.write_state_file({"last_file_timestamp": since_timestamp.isoformat(),
                                   "last_report_id": latest_report_id,
                                   "report_header": self.last_header,
                                   "report_column_slots": self.column_slots})

            logging.info(
                f"Extraction finished at {datetime.now().isoformat()}.")
        except Exception as e:
            raise e
        finally:
            self.duckdb_client.close()

    def _write_table_manifest(self, output_table, report_name):
        loading_options = self.configuration.parameters.get(KEY_LOADING_OPTIONS, {})
        incremental = bool(loading_options.get(
            KEY_LOADING_OPTIONS_INCREMENTAL_OUTPUT, False))
        pkey = loading_options.get(KEY_LOADING_OPTIONS_PKEY, [])

        # Create table definition using ComponentBase API
        # schema must be provided for projects with new-native-types feature enabled
        table_def = self.create_out_table_definition(
            name=f"{report_name}.csv",
            incremental=incremental,
            primary_key=pkey,
            schema=self.last_header,
            has_header=True
        )

        # Write manifest using ComponentBase method
        self.write_manifest(table_def)

    def _retrieve_report_manifests(self, all_files, report_name):
        manifests = []
        for obj in all_files:
            object_name = obj['Key'].split('/')[-1]
            parent_folder_name = obj['Key'].split('/')[-2]
            start_date, end_date = self._try_to_parse_report_period(
                parent_folder_name)
            # get only root (period) manifests
            manifest_file_name = f"{report_name}-Manifest.json"
            if start_date and object_name == manifest_file_name:
                # download file content
                manifest = json.loads(self._read_s3_file_contents(obj['Key']))
                manifest['last_modified'] = obj['LastModified']
                manifest['report_folder'] = obj['Key'].replace(
                    f'/{manifest_file_name}', '')
                manifest['period'] = parent_folder_name
                manifests.append(manifest)
        return self._dedupe_manifests_by_period(manifests)

    @staticmethod
    def _dedupe_manifests_by_period(manifests):
        """
        Keep only the latest manifest (by last_modified) for each billing period.

        AWS CUR regenerates entire billing periods when costs are updated retroactively.
        Each regeneration creates a new assemblyId for the same period. Without dedup,
        loading multiple versions of the same period causes duplicate rows.
        """
        latest_by_period = {}
        for m in manifests:
            period = m['period']
            if period not in latest_by_period or m['last_modified'] > latest_by_period[period]['last_modified']:
                if period in latest_by_period:
                    logging.info(
                        f"Replacing older report for period {period} "
                        f"(assembly {latest_by_period[period]['assemblyId']}) "
                        f"with newer version (assembly {m['assemblyId']})")
                latest_by_period[period] = m
        skipped = len(manifests) - len(latest_by_period)
        if skipped > 0:
            logging.info(f"Deduplicated manifests: kept {len(latest_by_period)} latest out of {len(manifests)} total "
                         f"({skipped} older versions skipped)")
        return list(latest_by_period.values())

    def _download_and_unzip(self, key: str, local_path) -> str:
        """
        Download ZIP file from S3, unzip
        Args:
            key:
            local_path:

        Returns:

        """
        self.s3_client.download_file(self.bucket, key, local_path)
        temp_dir = tempfile.mkdtemp(suffix='_report.zip')

        # unzip
        with zipfile.ZipFile(local_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        # get all files in temp dir
        files = os.listdir(temp_dir)
        return os.path.join(temp_dir, files[0])

    def _load_report_chunks_to_duckdb(self, manifest, table_name):
        logging.info(
            f"Loading report ID {manifest['assemblyId']} for period {manifest['period']}"
            f" in {len(manifest['reportKeys'])} report chunks.")
        # The physically-ordered output column names for this file, resolved by
        # case-preserved identity rather than by physical position when _get_max_header_
        # normalized built the output header. Reusing that single resolution is what
        # guarantees the names below are the ones the output header was built from. They are
        # applied as an explicit header override in DuckDB, so source columns that differ
        # only in letter case are kept distinct instead of being collapsed.
        column_names = manifest['resolved_columns']
        self._assert_names_loadable(column_names, self.last_header, manifest['period'])

        is_zip = True if manifest['reportKeys'] and manifest['reportKeys'][0].endswith('zip') else False
        if is_zip:
            logging.info("Processing zip file via local processing")

        for key in manifest['reportKeys']:
            # support for // syntax
            key_split = key.split('/')
            if '//' in manifest['report_folder']:
                key = f"{manifest['report_folder']}/{key_split[-2]}/{key_split[-1]}"

            # download
            s3_path = f's3://{self.bucket}/{key}'

            logging.info(f"Loading chunk {key_split[-1]}")
            if s3_path.endswith('.zip'):
                # download zip and extract
                res_gz = self._download_and_unzip(key, f'/tmp/{key_split[-1]}.zip')
                self.duckdb_client.load_csv_file(table_name, column_names, res_gz)
            else:
                # Load directly from S3 using DuckDB
                aws_params = self.configuration.parameters[KEY_AWS_PARAMS]
                self.duckdb_client.load_csv_from_s3(table_name,
                                                    column_names,
                                                    s3_path,
                                                    aws_params[KEY_AWS_API_KEY_ID],
                                                    aws_params[KEY_AWS_API_KEY_SECRET],
                                                    aws_params[KEY_AWS_REGION])

    def _read_s3_file_contents(self, key):
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            return response['Body'].read()
        except ClientError as error:
            if error.response['Error']['Code'] == 'NoSuchKey':
                logging.exception("The specified object was not found.")
            elif error.response['Error']['Code'] == 'AccessDenied':
                logging.exception(
                    "Permission to access the object from S3 is missing.")
            else:
                logging.exception(str(error))
            raise

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
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
        except ClientError as error:
            logging.error(f"Error occurred while listing S3 objects: {error}")
            raise

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

        if not self.report_prefix.endswith('*'):
            self.report_prefix = self.report_prefix + '*'

    def _get_max_header_normalized(self, manifests):
        # If no manifests provided, return header from state
        if not manifests:
            return self.last_header

        # Column names that a single report file of this run spells more than once. Only such
        # a collision can have produced a '_<n>' disambiguating suffix, so this is the
        # evidence for reading one back out of a header written by an earlier version — a
        # suffix-shaped name is otherwise just a tag whose own name ends in '_<n>'. Collected
        # across all manifests so every file is resolved against the same evidence.
        self.run_collision_bases = set()
        for m in manifests:
            counts = {}
            for name in self._get_manifest_source_columns(m):
                counts[name.lower()] = counts.get(name.lower(), 0) + 1
            self.run_collision_bases.update(lower for lower, count in counts.items() if count > 1)

        for m in manifests:
            # Each report file's column names are resolved exactly once, here, and kept on
            # the manifest for the loading step. Resolving twice risks the output header and
            # a file's header override disagreeing, which would silently drop that file's
            # data for the columns they disagree on.
            m['resolved_columns'] = self._resolve_column_names(self._get_manifest_source_columns(m))
            norm_cols = set(m['resolved_columns'])
            if not norm_cols.issubset(set(self.last_header)):
                norm_cols.update(set(self.last_header))
                self.last_header = list(norm_cols)
                self.last_header.sort()

        # Merge case-insensitive variants (e.g., "user_owner" and "user_Owner")
        # Keeps first occurrence after sort, discards case variants
        self.last_header = self._merge_case_variants(self.last_header)

        return self.last_header

    def _merge_case_variants(self, header):
        """
        Merges case-insensitive variants into single column (first after sort).
        Note: Input header is already sorted alphabetically (line 346),
        so uppercase variants appear before lowercase in ASCII order.
        Example: ["USER_owner", "user_owner"] -> ["USER_owner"]
        """
        seen_lower = set()
        result = []

        for c in header:
            c_lower = c.lower()
            if c_lower not in seen_lower:
                seen_lower.add(c_lower)
                result.append(c)  # Keep first after sort (uppercase letters first)
            # Subsequent case variants are discarded

        return result

    def _get_manifest_source_columns(self, manifest):
        # normalize
        man_cols = [col['category'] + '/' + col['name']
                    for col in manifest['columns']]
        return self._kbc_normalize_header(man_cols)

    def _kbc_normalize_header(self, header):
        normalized = []

        for h in header:
            new_h = h.replace('/', '__')
            new_h = re.sub("[^a-zA-Z\\d_]", "_", new_h)
            normalized.append(new_h)
        return normalized

    def _resolve_column_names(self, source_header):
        """
        Assign an output column name to every physical column of a report file.

        Columns are identified by their case-preserved normalized name, not by their
        physical position. AWS CUR reports can list two tags whose sanitized names differ
        only in letter case (e.g. ``resourceTags/user:Team`` for team ownership and
        ``resourceTags/user:team`` for a k8s pod label), and the physical order of such a
        pair is not stable across billing periods. Since the DuckDB header override is
        positional, resolving by identity is what keeps each tag's data under its own
        column whichever order a period happens to use.

        Which output column each source column owns is remembered in the state file
        (``report_column_slots``), so existing configurations keep the columns they already
        have and a variant that is new to the report is given an *additional* column —
        nothing is ever renamed, and no name has to be inferred twice. For configurations
        whose state predates that mapping, as much as possible is read back out of the output
        header; see ``_build_slot_registry``.

        The result is case-insensitively unique by construction, which is what DuckDB
        requires of a ``names=[...]`` override.
        """
        # Fold any case-ambiguous entries first: a header carried over from an older version
        # may hold two bare variants ("..._Team" and "..._team"), which cannot both exist as
        # DuckDB columns. This is the same folding _get_max_header_normalized applies to the
        # final header, so it is idempotent here.
        known = self._merge_case_variants(self.last_header)
        registry = self._build_slot_registry(known)
        canonical_map = {name.lower(): name for name in known}
        taken_lower = {name.lower() for name in known}
        for names in self.column_slots.values():
            taken_lower.update(name.lower() for name in names)

        positions_by_name = {}
        for position, name in enumerate(source_header):
            positions_by_name.setdefault(name.lower(), []).append(position)

        # A disambiguating suffix must not collide with a column this file actually has: the
        # report may well contain a tag whose own name ends in '_<n>'.
        reserved_lower = set(positions_by_name)
        resolved = [None] * len(source_header)
        claimed_lower = set()
        # Both the groups and the variants within a group are walked in sorted order, so the
        # outcome depends only on *which* columns the file has, never on their order.
        for lower_name in sorted(positions_by_name):
            positions = positions_by_name[lower_name]
            collides_in_file = len(positions) > 1

            variants = {}
            for position in positions:
                variants.setdefault(source_header[position], []).append(position)
            for variant in sorted(variants):
                owned = [name for name in registry.get(variant, []) if name.lower() not in claimed_lower]
                assigned = []
                for position in variants[variant]:
                    if owned:
                        # The column this spelling owns. Spellings that are identical in case
                        # too are indistinguishable, so their slots go out in physical order.
                        name = owned.pop(0)
                    elif collides_in_file:
                        # A spelling new to a name this file uses more than once needs a
                        # column of its own, or the two tags' data would be merged.
                        name = self._next_free_name(variant, taken_lower, reserved_lower)
                    else:
                        # The only spelling of this name in the file and no column on record:
                        # fold onto the canonical spelling, so a tag whose letter case merely
                        # drifts between periods stays in one column (unchanged behaviour).
                        name = canonical_map.get(lower_name)
                        if name is None or name.lower() in claimed_lower:
                            name = self._next_free_name(variant, taken_lower, reserved_lower)
                    resolved[position] = name
                    taken_lower.add(name.lower())
                    claimed_lower.add(name.lower())
                    assigned.append(name)
                self.column_slots[variant] = assigned

        return resolved

    def _build_slot_registry(self, known_header):
        """
        Work out which case variant owns which output column.

        Maps a case-preserved source column name to the output column(s) it owns.

        The mapping persisted in state is authoritative. For a configuration whose state
        predates it, as much as possible is read back out of the output header: an unsuffixed
        name unambiguously belongs to the source column spelled the same way, while a ``_<n>``
        suffix is only read as a disambiguated variant when a report file of this run really
        does use that name more than once — the only thing that can produce such a suffix.
        Without that evidence the name is indistinguishable from a tag whose own name ends in
        ``_<n>``, so it is taken at face value rather than guessed at.
        """
        known = set(known_header)
        registry = {source: [name for name in names if name in known]
                    for source, names in self.column_slots.items()}
        registry = {source: names for source, names in registry.items() if names}

        suffixed = {}
        for name in known_header:
            base, index = self._split_dedupe_suffix(name)
            if index == 0:
                # An unsuffixed name belongs to the column spelled exactly the same way; a
                # suffix is only ever appended, never removed, so this is not a guess.
                registry.setdefault(name, [name])
            else:
                suffixed.setdefault(base, []).append((index, name))

        for base, entries in suffixed.items():
            if base.lower() in self.run_collision_bases:
                # A report file of this run really does spell this name more than once, which
                # is the only way a suffix gets appended, so read these as its variants.
                registry.setdefault(base, [name for _, name in sorted(entries)])
            else:
                # No collision to disambiguate: take the names at face value.
                for _, name in entries:
                    registry.setdefault(name, [name])

        return registry

    @staticmethod
    def _split_dedupe_suffix(name):
        """
        Split an output column name into a possible source name and dedup index.

        Disambiguated names are built as ``<source name>_<index>``. Whether a given name was
        actually built that way cannot be told from the name alone — see
        ``_build_slot_registry`` for how the ambiguity is resolved.
        """
        match = DEDUPE_SUFFIX_PATTERN.match(name)
        if match:
            return match.group('base'), int(match.group('index'))
        return name, 0

    @staticmethod
    def _next_free_name(base, taken_lower, reserved_lower=frozenset()):
        """
        Return ``base``, or ``base_<n>`` with the lowest n that is still free.

        A disambiguated name has to avoid the columns already assigned (``taken_lower``) and
        the source column names of the file being resolved (``reserved_lower``), so that a
        generated suffix never steals the name of a real column.
        """
        if base.lower() not in taken_lower:
            return base
        index = 1
        while f"{base}_{index}".lower() in taken_lower | reserved_lower:
            index += 1
        return f"{base}_{index}"

    @staticmethod
    def _assert_names_loadable(column_names, header, period):
        """
        Internal invariants for a file's header override, both guaranteed by construction:

        - it must be case-insensitively unique, because DuckDB resolves identifiers
          case-insensitively and rejects a ``names=[...]`` override holding two names that
          differ only in letter case;
        - every name must exist in the output header, because the export projects the header
          and would otherwise drop that column's data without any error.

        These are tripwires against a future regression — not states a configuration can
        reach — so they raise an internal error rather than a UserException.
        """
        seen = set()
        known = set(header)
        for name in column_names:
            if name.lower() in seen:
                raise RuntimeError(
                    f"Internal error: resolved a duplicate output column '{name}' for period "
                    f"'{period}'. This is a bug in the column name resolution.")
            if name not in known:
                raise RuntimeError(
                    f"Internal error: resolved column '{name}' for period '{period}' is missing "
                    f"from the output header. This is a bug in the column name resolution.")
            seen.add(name.lower())

    # TODO: support for datatypes
    def _create_result_table(self, report_name, max_header):
        columns = []
        for h in max_header:
            columns.append({"name": h, "type": 'TEXT'})
        self.duckdb_client.create_table(report_name, columns)

    def get_date_period_converted(self, since, until):
        """
        Convert date strings to datetime objects using dateparser.

        Args:
            since: Start date string (e.g., "2000-01-01", "5 days ago", etc.)
            until: End date string (e.g., "now", "yesterday", "2024-01-01", etc.)

        Returns:
            Tuple of (start_date, end_date) as datetime objects
        """
        # Parse start date
        if since:
            start_date = self._parse_date(since)
            if not start_date:
                raise ValueError(f"Unable to parse start date: {since}")
        else:
            start_date = datetime(2000, 1, 1)

        # Parse end date
        if until and until != 'now':
            end_date = self._parse_date(until)
            if not end_date:
                raise ValueError(f"Unable to parse end date: {until}")
        else:
            end_date = datetime.now()

        return start_date, end_date

    @staticmethod
    def _parse_date(value):
        """
        Parse a date value, taking a plain ``YYYY-MM-DD`` date without dateparser.

        dateparser probes several candidate formats and emits a DeprecationWarning about
        an ambiguous day-of-month on every call — including for unambiguous dates, and
        therefore on every single run, since ``min_date_since`` defaults to a plain date.
        The warning reads like a component failure in the job log. Relative expressions
        such as ``5 days ago`` or ``yesterday`` keep going through dateparser.
        """
        try:
            return datetime.strptime(value, ISO_DATE_FORMAT)
        except (TypeError, ValueError):
            return dateparser.parse(value)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(1)
