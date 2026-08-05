'''
Created on 12. 11. 2018

@author: esner
'''
import os
import unittest
import warnings
from datetime import datetime

import dateparser
import mock
from freezegun import freeze_time

from component import Component


class TestComponent(unittest.TestCase):

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()


class TestCaseVariantColumnIdentity(unittest.TestCase):
    """Case-colliding CUR tag columns are identified by letter case, not by physical
    position, so a period that orders such a pair differently still lands each tag in its
    own column (CFTL-764 / SUPPORT-17242, follow-up to SUPPORT-17124)."""

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
    def _resolve(comp, manifest):
        """Replicates how _load_report_chunks_to_duckdb names a file's physical columns."""
        original_cols = [col["category"] + "/" + col["name"] for col in manifest["columns"]]
        column_names = comp._resolve_column_names(comp._kbc_normalize_header(original_cols))
        comp._assert_names_loadable(column_names, manifest["period"])
        return column_names

    def test_stale_single_variant_state_no_longer_aborts(self):
        """THE reported failure: state holds only the lowercase variant, then the report
        starts carrying both. Nothing is reordered — there is a single period in the run —
        yet 1.2.1 aborted with "Aborting to avoid writing swapped data"."""
        comp = self._component(["identity__LineItemId", "resourceTags__user_owner"])
        manifest = self._manifest(
            "20260101-20260201",
            [("identity", "LineItemId"), ("resourceTags", "user:Owner"), ("resourceTags", "user:owner")],
        )

        header = comp._get_max_header_normalized([manifest])
        resolved = self._resolve(comp, manifest)

        # The column this configuration already has keeps its name and its own data ...
        self.assertEqual(resolved[2], "resourceTags__user_owner")
        # ... and the case variant that is new to the report gets an ADDITIONAL column.
        self.assertEqual(resolved[1], "resourceTags__user_Owner_1")
        self.assertEqual(header, ["identity__LineItemId",
                                  "resourceTags__user_Owner_1",
                                  "resourceTags__user_owner"])

    def test_reordered_period_resolves_by_case_not_position(self):
        """Two periods list the colliding pair in OPPOSITE physical order: each tag must
        still resolve to the same output column in both."""
        comp = self._component()
        pair = [("resourceTags", "user:Team"), ("resourceTags", "user:team")]
        period_a = self._manifest("20260301-20260401", [("identity", "LineItemId")] + pair)
        period_b = self._manifest("20260401-20260501", [("identity", "LineItemId")] + pair[::-1])

        comp._get_max_header_normalized([period_a, period_b])
        resolved_a = self._resolve(comp, period_a)
        resolved_b = self._resolve(comp, period_b)

        # user:Team sits at index 1 in period A and at index 2 in period B (and vice versa
        # for user:team) -- both must map onto the same column name.
        self.assertEqual(resolved_a[1], resolved_b[2])
        self.assertEqual(resolved_a[2], resolved_b[1])
        self.assertNotEqual(resolved_a[1], resolved_a[2])

    def test_consistent_order_keeps_one_column_per_tag(self):
        """Two periods list the colliding pair in the SAME order (the normal case)."""
        comp = self._component()
        pair = [("resourceTags", "user:Team"), ("resourceTags", "user:team")]
        manifests = [
            self._manifest("20260301-20260401", [("identity", "LineItemId")] + pair),
            self._manifest("20260401-20260501", [("identity", "LineItemId")] + pair),
        ]

        header = comp._get_max_header_normalized(manifests)

        self.assertEqual(header, ["identity__LineItemId",
                                  "resourceTags__user_Team",
                                  "resourceTags__user_team_1"])
        for manifest in manifests:
            self.assertEqual(self._resolve(comp, manifest), header)

    def test_manifest_iteration_order_does_not_change_resolution(self):
        """The verdict must not depend on the order S3 happens to list the manifests in."""
        pair = [("resourceTags", "user:Team"), ("resourceTags", "user:team")]
        period_a = self._manifest("20260301-20260401", [("identity", "LineItemId")] + pair)
        period_b = self._manifest("20260401-20260501", [("identity", "LineItemId")] + pair[::-1])

        forward = self._component()
        forward_header = list(forward._get_max_header_normalized([period_a, period_b]))
        reverse = self._component()
        reverse_header = list(reverse._get_max_header_normalized([period_b, period_a]))

        self.assertEqual(forward_header, reverse_header)
        for manifest in (period_a, period_b):
            self.assertEqual(self._resolve(forward, manifest), self._resolve(reverse, manifest))

    def test_existing_mixed_case_state_names_are_preserved(self):
        """A configuration already loading both variants keeps the exact columns it has,
        whichever order the report lists them in -- no renames, so no schema break."""
        state = ["identity__LineItemId", "resourceTags__user_Team", "resourceTags__user_team_1"]
        pair = [("resourceTags", "user:Team"), ("resourceTags", "user:team")]

        for order, label in ((pair, "declared order"), (pair[::-1], "reversed order")):
            with self.subTest(label):
                comp = self._component(state)
                manifest = self._manifest("20260101-20260201", [("identity", "LineItemId")] + order)

                header = comp._get_max_header_normalized([manifest])
                resolved = self._resolve(comp, manifest)

                self.assertEqual(header, state)
                self.assertCountEqual(resolved, state)

    def test_cross_period_case_drift_still_folds_to_one_column(self):
        """Each period carries only ONE case variant of a tag: they must keep folding into
        a single column, exactly as before."""
        comp = self._component()
        period_a = self._manifest("20260301-20260401",
                                  [("identity", "LineItemId"), ("resourceTags", "user:Owner")])
        period_b = self._manifest("20260401-20260501",
                                  [("identity", "LineItemId"), ("resourceTags", "user:owner")])

        header = comp._get_max_header_normalized([period_a, period_b])

        self.assertEqual(header, ["identity__LineItemId", "resourceTags__user_Owner"])
        self.assertEqual(self._resolve(comp, period_a), header)
        self.assertEqual(self._resolve(comp, period_b), header)

    def test_resolved_names_are_case_insensitively_unique(self):
        """DuckDB rejects a header override holding two names that differ only in case."""
        comp = self._component(["resourceTags__user_Team", "resourceTags__user_team_1"])
        manifest = self._manifest(
            "20260101-20260201",
            [("resourceTags", "user:Team"), ("resourceTags", "user:team"), ("resourceTags", "user:TEAM")],
        )

        comp._get_max_header_normalized([manifest])
        resolved = self._resolve(comp, manifest)

        self.assertEqual(len({name.lower() for name in resolved}), len(resolved))

    def test_identical_duplicate_columns_get_distinct_names(self):
        """Columns with the same name AND case are indistinguishable, but must still be
        given separate output columns rather than collapsing."""
        comp = self._component()
        manifest = self._manifest(
            "20260101-20260201",
            [("resourceTags", "user:Team"), ("resourceTags", "user:Team")],
        )

        comp._get_max_header_normalized([manifest])
        resolved = self._resolve(comp, manifest)

        self.assertEqual(resolved, ["resourceTags__user_Team", "resourceTags__user_Team_1"])

    def test_legacy_ambiguous_state_does_not_break_loading(self):
        """State written before case variants were merged can hold two bare variants,
        which cannot both be DuckDB columns; loading must still resolve cleanly."""
        comp = self._component(["resourceTags__user_Team", "resourceTags__user_team"])
        manifest = self._manifest(
            "20260101-20260201",
            [("resourceTags", "user:Team"), ("resourceTags", "user:team")],
        )

        header = comp._get_max_header_normalized([manifest])
        resolved = self._resolve(comp, manifest)

        self.assertEqual(len({name.lower() for name in header}), len(header))
        for name in resolved:
            self.assertIn(name, header)


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
        self.assertEqual(comp._parse_date("2026-01-31"), dateparser.parse("2026-01-31"))

    def test_relative_dates_still_parse(self):
        comp = self._component()
        for value in ("5 days ago", "yesterday", "2026-01-31 10:00:00", "31/01/2026"):
            with self.subTest(value):
                self.assertIsNotNone(comp._parse_date(value))

    def test_unparseable_date_still_raises(self):
        comp = self._component()
        with self.assertRaises(ValueError):
            comp.get_date_period_converted("not-a-date-at-all", "now")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
