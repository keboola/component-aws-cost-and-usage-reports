'''
Created on 12. 11. 2018

@author: esner
'''
import os
import unittest

import mock
from freezegun import freeze_time
from keboola.component.exceptions import UserException

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


class TestColumnOrderSwapGuard(unittest.TestCase):
    """Guard against silently swapping data when a case-colliding tag pair is ordered
    differently across billing periods (SUPPORT-17124 follow-up)."""

    @staticmethod
    def _component():
        # Bypass __init__ (no config / S3 needed); we only call pure header helpers.
        comp = Component.__new__(Component)
        comp.last_header = []
        return comp

    @staticmethod
    def _manifest(period, columns):
        # columns: list of (category, name) tuples in physical order.
        return {"period": period, "columns": [{"category": c, "name": n} for c, n in columns]}

    def _build_column_names(self, comp, manifest):
        """Replicates how _load_report_chunks_to_duckdb builds the three lists per file."""
        original_cols = [col["category"] + "/" + col["name"] for col in manifest["columns"]]
        pre_dedup = comp._kbc_normalize_header(original_cols)
        deduped = comp._dedupe_header(pre_dedup)
        canonical_map = {c.lower(): c for c in comp.last_header}
        column_names = [canonical_map.get(n.lower(), n) for n in deduped]
        return pre_dedup, deduped, column_names

    def _check(self, comp, manifest):
        pre_dedup, deduped, column_names = self._build_column_names(comp, manifest)
        comp._guard_no_column_order_swap(pre_dedup, deduped, column_names, manifest["period"])

    def test_guard_allows_consistent_order(self):
        """Two periods list the colliding pair in the SAME order -> no error."""
        comp = self._component()
        pair = [("resourceTags", "user:Team"), ("resourceTags", "user:team")]
        manifests = [
            self._manifest("20260301-20260401", [("identity", "LineItemId")] + pair),
            self._manifest("20260401-20260501", [("identity", "LineItemId")] + pair),
        ]
        comp._get_max_header_normalized(manifests)
        for man in manifests:
            self._check(comp, man)  # must not raise

    def test_guard_raises_on_crossed_order(self):
        """A later period lists the colliding pair in the OPPOSITE order -> abort."""
        comp = self._component()
        period_a = self._manifest(
            "20260301-20260401",
            [("identity", "LineItemId"), ("resourceTags", "user:Team"), ("resourceTags", "user:team")],
        )
        period_b = self._manifest(
            "20260401-20260501",
            [("identity", "LineItemId"), ("resourceTags", "user:team"), ("resourceTags", "user:Team")],
        )
        comp._get_max_header_normalized([period_a, period_b])
        # The canonical-order period is fine ...
        self._check(comp, period_a)
        # ... the opposite-order period must fail loudly instead of swapping data.
        with self.assertRaises(UserException):
            self._check(comp, period_b)

    def test_guard_ignores_benign_single_variant_merge(self):
        """Each period has only ONE case variant of a tag (merged across periods by
        _merge_case_variants) -> not a within-file collision, so the guard never fires."""
        comp = self._component()
        period_a = self._manifest(
            "20260301-20260401", [("identity", "LineItemId"), ("resourceTags", "user:Owner")]
        )
        period_b = self._manifest(
            "20260401-20260501", [("identity", "LineItemId"), ("resourceTags", "user:owner")]
        )
        comp._get_max_header_normalized([period_a, period_b])
        for man in (period_a, period_b):
            self._check(comp, man)  # must not raise


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
