import re
from typing import List, Dict


class ColumnNormalizer:
    """Handles column name normalization and deduplication for KBC Storage compatibility."""

    def normalize_column_names_for_kbc(self, column_names: List[str]) -> List[str]:
        """Normalize AWS report column names to meet Keboola Connection Storage requirements."""
        normalized_names = []

        for column_name in column_names:
            # Replace category separator (/) with double underscore
            normalized = column_name.replace("/", "__")

            # Replace any remaining special characters with underscore
            normalized = re.sub("[^a-zA-Z\\d_]", "_", normalized)

            normalized_names.append(normalized)

        return normalized_names

    def remove_duplicate_column_names(
        self, column_names: List[str], separator: str = "_"
    ) -> List[str]:
        """Remove duplicate column names by adding numeric suffixes."""
        unique_names = []
        seen_lowercase = []
        duplicate_counters = {}

        for name in column_names:
            lowercase_name = name.lower()

            if lowercase_name in seen_lowercase:
                # This is a duplicate - add numeric suffix
                counter = duplicate_counters.get(lowercase_name, 0) + 1
                unique_name = f"{name}{separator}{counter}"
                duplicate_counters[lowercase_name] = counter
            else:
                # First occurrence of this name
                unique_name = name
                seen_lowercase.append(lowercase_name)

            unique_names.append(unique_name)

        return unique_names

    def get_manifest_normalized_columns(self, manifest: Dict) -> List[str]:
        """Extract and normalize column names from manifest metadata."""
        # Build column names from manifest metadata
        manifest_columns = [
            col["category"] + "/" + col["name"] for col in manifest["columns"]
        ]

        # Normalize column names for KBC Storage
        normalized_columns = self.normalize_column_names_for_kbc(manifest_columns)

        # Remove duplicates
        return self.remove_duplicate_column_names(normalized_columns)

    def normalize_and_merge_columns(
        self, current_columns: List[str], historical_columns: List[str]
    ) -> List[str]:
        """Normalize column names and merge with historical columns."""
        # Normalize column names for KBC Storage compatibility
        normalized = self.normalize_column_names_for_kbc(current_columns)
        normalized = self.remove_duplicate_column_names(normalized)

        # Merge with historical columns
        all_columns = set(normalized)
        all_columns.update(historical_columns)

        return sorted(list(all_columns))

    def build_column_aligned_select(
        self, target_columns: List[str], source_columns: List[str]
    ) -> List[str]:
        """Build SELECT statement parts that align source columns with target table schema."""
        select_parts = []

        for target_column in target_columns:
            # Convert target column name back to original format
            # e.g. 'identity__LineItemId' -> 'identity/LineItemId'
            original_column = target_column.replace("__", "/")

            if original_column in source_columns:
                select_parts.append(f'"{original_column}"')
            else:
                # Column not present in this chunk - use NULL
                select_parts.append(f'NULL AS "{target_column}"')

        return select_parts

    def get_max_header_normalized(
        self, manifests: List[Dict], current_header: List[str]
    ) -> List[str]:
        """Build normalized column schema from all manifests."""
        header = current_header.copy()

        for manifest in manifests:
            # Get normalized columns from this manifest
            normalized_columns = self.get_manifest_normalized_columns(manifest)

            # Merge with existing header
            norm_cols = set(normalized_columns)
            if not norm_cols.issubset(set(header)):
                norm_cols.update(set(header))
                header = list(norm_cols)
                header.sort()

        return header
