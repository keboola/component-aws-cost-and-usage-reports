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


class ColumnResolutionTestCase(unittest.TestCase):
    """Shared setup for the column-name resolution tests."""

    @staticmethod
    def _component(state_header=None, column_slots=None):
        # Bypass __init__ (no config / S3 needed); we only call pure header helpers.
        comp = Component.__new__(Component)
        comp.last_header = list(state_header) if state_header else []
        comp.column_slots = {source: list(names) for source, names in (column_slots or {}).items()}
        comp.run_source_names = set()
        return comp

    @staticmethod
    def _manifest(period, columns):
        # columns: list of (category, name) tuples in physical order.
        return {"period": period, "columns": [{"category": c, "name": n} for c, n in columns]}

    @staticmethod
    def _resolve(comp, manifests):
        """Derive the header and each file's column names exactly as run() does, including
        the invariants the loading step asserts."""
        header = comp._get_max_header_normalized(manifests)
        for manifest in manifests:
            comp._assert_names_loadable(manifest["resolved_columns"], header, manifest["period"])
        return header, [manifest["resolved_columns"] for manifest in manifests]


class TestCaseVariantColumnIdentity(ColumnResolutionTestCase):
    """Case-colliding CUR tag columns are identified by letter case, not by physical
    position, so a period that orders such a pair differently still lands each tag in its
    own column (CFTL-764 / SUPPORT-17242, follow-up to SUPPORT-17124)."""

    def test_stale_single_variant_state_no_longer_aborts(self):
        """THE reported failure: state holds only the lowercase variant, then the report
        starts carrying both. Nothing is reordered — there is a single period in the run —
        yet 1.2.1 aborted with "Aborting to avoid writing swapped data"."""
        comp = self._component(["identity__LineItemId", "resourceTags__user_owner"])
        manifest = self._manifest(
            "20260101-20260201",
            [("identity", "LineItemId"), ("resourceTags", "user:Owner"), ("resourceTags", "user:owner")],
        )

        header, (resolved,) = self._resolve(comp, [manifest])

        # The column this configuration already has keeps its name and its own data ...
        self.assertEqual(resolved[2], "resourceTags__user_owner")
        # ... and the case variant that is new to the report gets an ADDITIONAL column.
        self.assertEqual(resolved[1], "resourceTags__user_Owner_1")
        self.assertEqual(header, ["identity__LineItemId",
                                  "resourceTags__user_Owner_1",
                                  "resourceTags__user_owner"])
        # The assignment is remembered, so later runs do not have to infer it again.
        self.assertEqual(comp.column_slots["resourceTags__user_owner"], ["resourceTags__user_owner"])
        self.assertEqual(comp.column_slots["resourceTags__user_Owner"], ["resourceTags__user_Owner_1"])

    def test_reordered_period_resolves_by_case_not_position(self):
        """Two periods list the colliding pair in OPPOSITE physical order: each tag must
        still resolve to the same output column in both."""
        comp = self._component()
        pair = [("resourceTags", "user:Team"), ("resourceTags", "user:team")]
        period_a = self._manifest("20260301-20260401", [("identity", "LineItemId")] + pair)
        period_b = self._manifest("20260401-20260501", [("identity", "LineItemId")] + pair[::-1])

        _, (resolved_a, resolved_b) = self._resolve(comp, [period_a, period_b])

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

        header, resolved = self._resolve(comp, manifests)

        self.assertEqual(header, ["identity__LineItemId",
                                  "resourceTags__user_Team",
                                  "resourceTags__user_team_1"])
        for names in resolved:
            self.assertEqual(names, header)

    def test_manifest_order_does_not_matter_when_every_period_has_both_variants(self):
        """Every period carries both variants, so the outcome must not depend on the order
        S3 happens to list the manifests in. (Which spelling wins the shared column when the
        periods carry only ONE variant each is order-dependent, as it has always been.)"""
        pair = [("resourceTags", "user:Team"), ("resourceTags", "user:team")]
        period_a = self._manifest("20260301-20260401", [("identity", "LineItemId")] + pair)
        period_b = self._manifest("20260401-20260501", [("identity", "LineItemId")] + pair[::-1])

        forward_header, forward = self._resolve(self._component(), [period_a, period_b])
        reverse_header, reverse = self._resolve(self._component(), [period_b, period_a])

        self.assertEqual(forward_header, reverse_header)
        self.assertEqual(forward, list(reversed(reverse)))

    def test_existing_mixed_case_state_names_are_preserved(self):
        """A configuration already loading both variants keeps the exact columns it has,
        whichever order the report lists them in -- no renames, so no schema break."""
        state = ["identity__LineItemId", "resourceTags__user_Team", "resourceTags__user_team_1"]
        pair = [("resourceTags", "user:Team"), ("resourceTags", "user:team")]

        for order, label in ((pair, "declared order"), (pair[::-1], "reversed order")):
            with self.subTest(label):
                comp = self._component(state)
                manifest = self._manifest("20260101-20260201", [("identity", "LineItemId")] + order)

                header, (resolved,) = self._resolve(comp, [manifest])

                self.assertEqual(header, state)
                self.assertCountEqual(resolved, state)

    def test_legacy_header_recovers_which_variant_owns_which_column(self):
        """State written before the mapping was persisted: a period carrying only ONE of the
        two variants must still resolve to that variant's own column."""
        comp = self._component(["resourceTags__user_Team", "resourceTags__user_team_1"])
        manifest = self._manifest("20260101-20260201", [("resourceTags", "user:team")])

        _, (resolved,) = self._resolve(comp, [manifest])

        self.assertEqual(resolved, ["resourceTags__user_team_1"])

    def test_recorded_slots_survive_a_period_dropping_a_variant(self):
        """Same, driven by the mapping persisted in state rather than recovered."""
        comp = self._component(
            ["resourceTags__user_Team", "resourceTags__user_team_1"],
            {"resourceTags__user_Team": ["resourceTags__user_Team"],
             "resourceTags__user_team": ["resourceTags__user_team_1"]},
        )
        manifest = self._manifest("20260101-20260201", [("resourceTags", "user:Team")])

        _, (resolved,) = self._resolve(comp, [manifest])

        self.assertEqual(resolved, ["resourceTags__user_Team"])

    def test_later_period_variant_does_not_orphan_an_earlier_period_column(self):
        """The output header grows while the manifests are resolved, so a period resolved
        early must keep a name that is still in the final header -- otherwise the export
        projection drops that period's data for that column, with no error at all."""
        comp = self._component(["resourceTags__user_Team"])
        period_a = self._manifest("20260301-20260401", [("resourceTags", "user:team")])
        period_b = self._manifest(
            "20260401-20260501",
            [("resourceTags", "user:Team"), ("resourceTags", "user:TEAM")],
        )

        header, (resolved_a, resolved_b) = self._resolve(comp, [period_a, period_b])

        self.assertEqual(resolved_a, ["resourceTags__user_Team"])
        for names in (resolved_a, resolved_b):
            for name in names:
                self.assertIn(name, header)

    def test_cross_period_case_drift_still_folds_to_one_column(self):
        """Each period carries only ONE case variant of a tag: they must keep folding into
        a single column, exactly as before."""
        comp = self._component()
        period_a = self._manifest("20260301-20260401",
                                  [("identity", "LineItemId"), ("resourceTags", "user:Owner")])
        period_b = self._manifest("20260401-20260501",
                                  [("identity", "LineItemId"), ("resourceTags", "user:owner")])

        header, resolved = self._resolve(comp, [period_a, period_b])

        self.assertEqual(header, ["identity__LineItemId", "resourceTags__user_Owner"])
        for names in resolved:
            self.assertEqual(names, header)

    def test_source_column_ending_in_a_number_is_not_read_as_a_variant(self):
        """A tag genuinely named `tier_1` must not be mistaken for the disambiguated form of
        `tier`, which would hand `tier`'s data to the `tier_1` tag's column."""
        comp = self._component(["resourceTags__user_tier_1", "resourceTags__user_tier_2"])
        manifest = self._manifest("20260101-20260201", [("resourceTags", "user:tier")])

        header, (resolved,) = self._resolve(comp, [manifest])

        self.assertEqual(resolved, ["resourceTags__user_tier"])
        self.assertIn("resourceTags__user_tier_1", header)
        self.assertIn("resourceTags__user_tier_2", header)

    def test_source_columns_named_x_and_x_1_keep_separate_columns(self):
        """`tier` and `tier_1` are different tags and must not be resolved to one column."""
        comp = self._component(["resourceTags__user_tier_1", "resourceTags__user_tier_2"])
        manifest = self._manifest(
            "20260101-20260201",
            [("resourceTags", "user:tier"), ("resourceTags", "user:tier_1")],
        )

        _, (resolved,) = self._resolve(comp, [manifest])

        self.assertEqual(resolved, ["resourceTags__user_tier", "resourceTags__user_tier_1"])

    def test_generated_suffix_never_steals_a_real_column_name(self):
        """A case collision on `tier` needs a suffix, but `tier_1` is a real column of the
        same report -- the generated name must step over it."""
        comp = self._component()
        manifest = self._manifest(
            "20260101-20260201",
            [("resourceTags", "user:tier"), ("resourceTags", "user:tier_1"), ("resourceTags", "user:TIER")],
        )

        header, (resolved,) = self._resolve(comp, [manifest])

        # The real `tier_1` tag keeps its own name ...
        self.assertEqual(resolved[1], "resourceTags__user_tier_1")
        # ... so the disambiguated `tier` has to take the next free one.
        self.assertEqual(resolved[0], "resourceTags__user_tier_2")
        self.assertEqual(resolved[2], "resourceTags__user_TIER")
        self.assertEqual(len({name.lower() for name in resolved}), len(resolved))

    def test_a_real_tag_named_like_a_suffix_keeps_its_own_column(self):
        """A configuration whose report has BOTH a case-colliding pair and an ordinary tag
        that sanitizes to `<base>_<n>` (e.g. `user:tier-1`). The ordinary tag's column must
        not be mistaken for the disambiguated form of `user:tier` and handed its data."""
        state = ["identity__LineItemId", "resourceTags__user_Tier", "resourceTags__user_tier_1"]
        comp = self._component(state)
        manifest = self._manifest("20260101-20260201", [("identity", "LineItemId"),
                                                        ("resourceTags", "user:tier"),
                                                        ("resourceTags", "user:tier-1")])

        header, (resolved,) = self._resolve(comp, [manifest])

        self.assertEqual(resolved[2], "resourceTags__user_tier_1")
        self.assertNotEqual(resolved[1], "resourceTags__user_tier_1")
        self.assertEqual(header, sorted(state))

    def test_repeated_runs_do_not_grow_the_output_header(self):
        """Running the same report again must not invent another column each time."""
        state = ["identity__LineItemId", "resourceTags__user_Tier", "resourceTags__user_tier_1"]
        columns = [("identity", "LineItemId"),
                   ("resourceTags", "user:tier"),
                   ("resourceTags", "user:tier-1")]

        header, slots = sorted(state), {}
        headers = []
        for _ in range(4):
            comp = self._component(header, slots)
            header, _ = self._resolve(comp, [self._manifest("20260101-20260201", columns)])
            slots = comp.column_slots
            headers.append(list(header))

        self.assertEqual(headers[0], headers[-1], f"output columns grow every run: {headers}")

    def test_resolution_is_stable_across_runs(self):
        """The second run starts from the state the first one wrote and must resolve every
        column to the same place."""
        first = self._component(["resourceTags__user_owner"])
        manifest = self._manifest(
            "20260101-20260201",
            [("resourceTags", "user:Owner"), ("resourceTags", "user:owner")],
        )

        header, (resolved,) = self._resolve(first, [manifest])
        second = self._component(header, first.column_slots)
        header_again, (resolved_again,) = self._resolve(second, [self._manifest(
            "20260101-20260201",
            [("resourceTags", "user:Owner"), ("resourceTags", "user:owner")],
        )])

        self.assertEqual(resolved_again, resolved)
        self.assertEqual(header_again, header)

    def test_resolved_names_are_case_insensitively_unique(self):
        """DuckDB rejects a header override holding two names that differ only in case."""
        comp = self._component(["resourceTags__user_Team", "resourceTags__user_team_1"])
        manifest = self._manifest(
            "20260101-20260201",
            [("resourceTags", "user:Team"), ("resourceTags", "user:team"), ("resourceTags", "user:TEAM")],
        )

        _, (resolved,) = self._resolve(comp, [manifest])

        self.assertEqual(len({name.lower() for name in resolved}), len(resolved))

    def test_identical_duplicate_columns_get_distinct_names(self):
        """Columns with the same name AND case are indistinguishable, but must still be
        given separate output columns rather than collapsing."""
        comp = self._component()
        manifest = self._manifest(
            "20260101-20260201",
            [("resourceTags", "user:Team"), ("resourceTags", "user:Team")],
        )

        _, (resolved,) = self._resolve(comp, [manifest])

        self.assertEqual(resolved, ["resourceTags__user_Team", "resourceTags__user_Team_1"])

    def test_legacy_ambiguous_state_does_not_break_loading(self):
        """State written before case variants were merged can hold two bare variants, which
        cannot both be DuckDB columns; loading must still resolve cleanly."""
        comp = self._component(["resourceTags__user_Team", "resourceTags__user_team"])
        manifest = self._manifest(
            "20260101-20260201",
            [("resourceTags", "user:Team"), ("resourceTags", "user:team")],
        )

        header, _ = self._resolve(comp, [manifest])

        self.assertEqual(len({name.lower() for name in header}), len(header))


class TestCaseVariantColumnData(ColumnResolutionTestCase):
    """End-to-end: resolve the header, load each period with its own override, export --
    and check the values land under the right columns."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _write_csv(self, name, rows):
        path = os.path.join(self.tmp_dir, name)
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)
        return path

    def _export(self, comp, manifests, chunks):
        """Load every manifest with the names the component resolved for it, then export."""
        header, resolved = self._resolve(comp, manifests)
        client = DuckDBClient()
        try:
            client.create_table("report", [{"name": c, "type": "TEXT"} for c in header])
            for names, path in zip(resolved, chunks):
                client.load_csv_file("report", names, path)
            out_path = os.path.join(self.tmp_dir, "out.csv")
            client.export_to_csv("report", out_path, header)
        finally:
            client.close()
        with open(out_path, encoding="utf-8") as f:
            rows = list(csv.reader(f))
        return rows[0], {r[0]: dict(zip(rows[0][1:], r[1:])) for r in rows[1:]}

    def test_reordered_period_yields_correct_not_swapped_data(self):
        """A period listing the colliding pair in the OPPOSITE order must still put each
        tag's values under its own column -- the swap the 1.1.6 guard aborted on."""
        comp = self._component()
        pair = [("resourceTags", "user:Team"), ("resourceTags", "user:team")]
        period_a = self._manifest("20260301-20260401", [("identity", "LineItemId")] + pair)
        period_b = self._manifest("20260401-20260501", [("identity", "LineItemId")] + pair[::-1])
        chunk_a = self._write_csv("period_a.csv", [
            ["identity/LineItemId", "resourceTags/user:Team", "resourceTags/user:team"],
            ["li1", "platform", "pod-a"]])
        chunk_b = self._write_csv("period_b.csv", [
            ["identity/LineItemId", "resourceTags/user:team", "resourceTags/user:Team"],
            ["li2", "pod-b", "data"]])

        header, rows = self._export(comp, [period_a, period_b], [chunk_a, chunk_b])

        team_column, pod_column = header[1], header[2]
        self.assertEqual(rows["li1"][team_column], "platform")
        self.assertEqual(rows["li1"][pod_column], "pod-a")
        # Period B is the reordered one: the ownership tag keeps "data" and the pod label
        # keeps "pod-b" instead of being swapped.
        self.assertEqual(rows["li2"][team_column], "data")
        self.assertEqual(rows["li2"][pod_column], "pod-b")

    def test_no_period_loses_data_when_a_later_period_adds_a_variant(self):
        """Regression for a silent loss: a period resolved before the header finished growing
        must still have its values exported."""
        comp = self._component(["resourceTags__user_Team"])
        period_a = self._manifest("20260301-20260401", [("identity", "LineItemId"),
                                                        ("resourceTags", "user:team")])
        period_b = self._manifest("20260401-20260501", [("identity", "LineItemId"),
                                                        ("resourceTags", "user:Team"),
                                                        ("resourceTags", "user:TEAM")])
        chunk_a = self._write_csv("a.csv", [
            ["identity/LineItemId", "resourceTags/user:team"], ["liA", "value-a"]])
        chunk_b = self._write_csv("b.csv", [
            ["identity/LineItemId", "resourceTags/user:Team", "resourceTags/user:TEAM"],
            ["liB", "value-b", "value-upper"]])

        header, resolved = self._resolve(comp, [period_a, period_b])
        _, rows = self._export(self._component(["resourceTags__user_Team"]),
                               [period_a, period_b], [chunk_a, chunk_b])

        # Each value must sit in the column its own period was resolved to, not merely
        # "somewhere in the row".
        self.assertEqual(rows["liA"][resolved[0][1]], "value-a")
        self.assertEqual(rows["liB"][resolved[1][1]], "value-b")
        self.assertEqual(rows["liB"][resolved[1][2]], "value-upper")
        self.assertEqual(len(header), len(set(header)))


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
