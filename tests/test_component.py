'''
Created on 12. 11. 2018

@author: esner
'''
import csv
import os
import shutil
import tempfile
import unittest
import warnings
from datetime import datetime

import dateparser
import mock
from freezegun import freeze_time

from component import Component
from duckdb_client import DuckDBClient


class TestComponent(unittest.TestCase):

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()


class TestCaseDifferingColumns(unittest.TestCase):
    """Reports whose tag names differ only in letter case (SUPPORT-17124) are read with an
    explicit per-file header override so DuckDB does not collapse them. 1.2.1 also aborted
    the run when such a pair *looked* reordered across periods; that guard is gone
    (CFTL-764 / SUPPORT-17242) because it fired on reports that were never reordered."""

    @staticmethod
    def _component(state_header=None):
        # Bypass __init__ (no config / S3 needed); we only call pure header helpers.
        comp = Component.__new__(Component)
        comp.last_header = list(state_header) if state_header else []
        return comp

    @staticmethod
    def _manifest(period, columns):
        # columns: list of (category, name) tuples in physical order.
        return {"period": period, "columns": [{"category": c, "name": n} for c, n in columns]}

    @staticmethod
    def _column_names(comp, manifest):
        """Replicates how _load_report_chunks_to_duckdb names a file's physical columns."""
        original_cols = [col["category"] + "/" + col["name"] for col in manifest["columns"]]
        deduped = comp._dedupe_header(comp._kbc_normalize_header(original_cols))
        canonical_map = {c.lower(): c for c in comp.last_header}
        return [canonical_map.get(name.lower(), name) for name in deduped]

    def _resolve(self, comp, manifests):
        header = comp._get_max_header_normalized(manifests)
        return header, [self._column_names(comp, manifest) for manifest in manifests]

    def test_stale_single_variant_state_no_longer_aborts(self):
        """THE reported failure. State holds only the lowercase variant and the report then
        carries both. Nothing is reordered -- there is one period in the run -- yet 1.2.1
        aborted with "Aborting to avoid writing swapped data"."""
        comp = self._component(["identity__LineItemId", "resourceTags__user_owner"])
        manifest = self._manifest(
            "20260101-20260201",
            [("identity", "LineItemId"), ("resourceTags", "user:Owner"), ("resourceTags", "user:owner")],
        )

        header, (names,) = self._resolve(comp, [manifest])

        # No exception, and the pair stays distinct so DuckDB will not collapse it.
        self.assertEqual(len({name.lower() for name in names}), len(names))
        for name in names:
            self.assertIn(name, header)

    def test_reordered_period_loads_instead_of_aborting(self):
        """A period listing the colliding pair in the OPPOSITE physical order used to abort
        the whole run. It now loads.

        Accepted trade-off: the header override stays positional, so for a report that
        genuinely reorders such a pair the two tags' values are exchanged for the reordered
        period. Releases before 1.2.1 mishandled this input too, and worse -- they collapsed
        the pair and lost one tag's values outright. An abort is preferred to neither, since
        it also fires on reports which are NOT reordered and leaves the configuration
        permanently failing with no action available to support."""
        comp = self._component()
        pair = [("resourceTags", "user:Team"), ("resourceTags", "user:team")]
        period_a = self._manifest("20260301-20260401", [("identity", "LineItemId")] + pair)
        period_b = self._manifest("20260401-20260501", [("identity", "LineItemId")] + pair[::-1])

        header, (names_a, names_b) = self._resolve(comp, [period_a, period_b])

        for names in (names_a, names_b):
            self.assertEqual(len({name.lower() for name in names}), len(names))
            for name in names:
                self.assertIn(name, header)

    def test_consistent_order_keeps_one_column_per_tag(self):
        """Two periods list the colliding pair in the SAME order (the normal case)."""
        comp = self._component()
        pair = [("resourceTags", "user:Team"), ("resourceTags", "user:team")]
        manifests = [
            self._manifest("20260301-20260401", [("identity", "LineItemId")] + pair),
            self._manifest("20260401-20260501", [("identity", "LineItemId")] + pair),
        ]

        header, resolved = self._resolve(comp, manifests)

        self.assertEqual(header, ["identity__LineItemId",
                                  "resourceTags__user_Team",
                                  "resourceTags__user_team_1"])
        for names in resolved:
            self.assertEqual(names, header)

    def test_cross_period_case_drift_folds_to_one_column(self):
        """Each period carries only ONE case variant of a tag: they fold into one column."""
        comp = self._component()
        period_a = self._manifest("20260301-20260401",
                                  [("identity", "LineItemId"), ("resourceTags", "user:Owner")])
        period_b = self._manifest("20260401-20260501",
                                  [("identity", "LineItemId"), ("resourceTags", "user:owner")])

        header, resolved = self._resolve(comp, [period_a, period_b])

        self.assertEqual(header, ["identity__LineItemId", "resourceTags__user_Owner"])
        for names in resolved:
            self.assertEqual(names, header)


class TestReportLoadingEndToEnd(unittest.TestCase):
    """Drives the real _load_report_chunks_to_duckdb and a real DuckDBClient, so the suite
    fails if an aborting guard is reintroduced on this path. The tests above exercise the
    header helpers only, which cannot detect that."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _write_csv(self, name, rows):
        path = os.path.join(self.tmp_dir, name)
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)
        return path

    def _load(self, state_header, periods):
        """periods: (period, [(category, name), ...], [data rows]) tuples in run order.

        Report chunks are served from local files by stubbing the S3 download, which is the
        only thing standing between this and a real run.
        """
        comp = Component.__new__(Component)
        comp.last_header = list(state_header)
        comp.bucket = "test-bucket"
        comp.duckdb_client = DuckDBClient()

        manifests, chunks = [], {}
        for period, columns, rows in periods:
            chunks[period] = self._write_csv(
                f"{period}.csv", [[c + "/" + n for c, n in columns]] + rows)
            manifests.append({
                "period": period,
                "assemblyId": f"assembly-{period}",
                "report_folder": f"reports/{period}",
                "reportKeys": [f"reports/{period}/chunk.zip"],
                "columns": [{"category": c, "name": n} for c, n in columns],
            })
        comp._download_and_unzip = lambda key, local_path: chunks[key.split("/")[-2]]

        max_header = comp._get_max_header_normalized(manifests)
        comp.duckdb_client.open_connection()
        try:
            comp._create_result_table("report", max_header)
            for manifest in manifests:
                comp._load_report_chunks_to_duckdb(manifest, "report")
            out_path = os.path.join(self.tmp_dir, "out.csv")
            comp.duckdb_client.export_to_csv("report", out_path, max_header)
        finally:
            comp.duckdb_client.close()

        with open(out_path, encoding="utf-8") as f:
            rows = list(csv.reader(f))
        return rows[0], {row[0]: dict(zip(rows[0], row)) for row in rows[1:]}

    def test_report_carrying_both_case_variants_loads(self):
        """THE reported failure, through the production loading path. 1.2.1 raises
        UserException here; every release before it loaded the report."""
        header, rows = self._load(
            ["identity__LineItemId", "resourceTags__user_owner"],
            [("20260101-20260201",
              [("identity", "LineItemId"),
               ("resourceTags", "user:Owner"),
               ("resourceTags", "user:owner")],
              [["li1", "UPPER-VALUE", "lower-value"]])],
        )

        self.assertEqual(header, ["identity__LineItemId",
                                  "resourceTags__user_owner",
                                  "resourceTags__user_owner_1"])
        # The physically-first variant takes the column the configuration already has, and
        # the second gets its own -- both tags keep their own value, neither is collapsed.
        self.assertEqual(rows["li1"]["resourceTags__user_owner"], "UPPER-VALUE")
        self.assertEqual(rows["li1"]["resourceTags__user_owner_1"], "lower-value")

    def test_period_listing_the_pair_in_the_other_order_loads(self):
        """1.2.1 aborts the whole run on the second period here.

        It now loads, and this test pins the accepted trade-off: because the header override
        is positional, the reordered period's two values ARE exchanged relative to the first
        period's. That is preferred over an abort which also fires on reports that were never
        reordered and which leaves the configuration permanently failing."""
        pair = [("resourceTags", "user:Team"), ("resourceTags", "user:team")]
        header, rows = self._load(
            [],
            [("20260301-20260401", [("identity", "LineItemId")] + pair, [["li1", "TEAM-a", "team-a"]]),
             ("20260401-20260501", [("identity", "LineItemId")] + pair[::-1], [["li2", "team-b", "TEAM-b"]])],
        )

        self.assertEqual(header, ["identity__LineItemId",
                                  "resourceTags__user_Team",
                                  "resourceTags__user_team_1"])
        self.assertEqual(rows["li1"]["resourceTags__user_Team"], "TEAM-a")
        self.assertEqual(rows["li2"]["resourceTags__user_Team"], "team-b")


class TestDateParsing(unittest.TestCase):
    """min_date_since defaults to a plain date, so dateparser's ambiguous-day
    DeprecationWarning was printed on every single run and read like a failure."""

    @staticmethod
    def _component():
        return Component.__new__(Component)

    def test_plain_dates_emit_no_deprecation_warning(self):
        comp = self._component()
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            start_date, end_date = comp.get_date_period_converted("2000-01-01", "2026-01-31")

        self.assertEqual(start_date, datetime(2000, 1, 1))
        self.assertEqual(end_date, datetime(2026, 1, 31))

    def test_plain_date_matches_previous_dateparser_result(self):
        comp = self._component()
        for value in ("2026-01-31", "2000-01-01", "2026-1-3"):
            with self.subTest(value):
                self.assertEqual(comp._parse_date(value), dateparser.parse(value))

    def test_relative_dates_still_parse(self):
        comp = self._component()
        for value in ("5 days ago", "yesterday", "1 month ago", "2026-01-31 10:00:00", "31/01/2026"):
            with self.subTest(value):
                self.assertIsNotNone(comp._parse_date(value))

    def test_unparseable_date_still_raises(self):
        comp = self._component()
        with self.assertRaises(ValueError):
            comp.get_date_period_converted("not-a-date-at-all", "now")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
