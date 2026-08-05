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
        genuinely reorders such a pair the two tags' values are swapped for the reordered
        period. That is the behaviour of every release before 1.2.1, and it is preferred over
        an abort that also fires on reports which are NOT reordered and which leaves the
        configuration permanently failing with no way for support to act."""
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
