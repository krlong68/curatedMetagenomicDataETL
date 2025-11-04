#!/usr/bin/env python3
"""
Load sample_id_map.csv into BigQuery as src_sample_id_map.

Follows the existing load utilities' conventions:
- Same GCP project and dataset IDs
- Clear step-by-step logging
- Truncate-and-reload behavior

This loader reads the local CSV (checked into the repo) and loads it directly
to BigQuery using the Python client. The table will be created if it doesn't
already exist.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

from google.cloud import bigquery


# Configuration (kept consistent with other load scripts)
PROJECT_ID = "curatedmetagenomicdata"
DATASET_ID = "curatedmetagenomicsdata"
TABLE_ID = "src_sample_id_map"

# Local file path (relative to this script's directory)
LOCAL_CSV = Path(__file__).resolve().parent / "sample_id_map.csv"


def load_to_bigquery(csv_path: Path) -> Optional[bigquery.Table]:
    """Load the local CSV into BigQuery, replacing existing data.

    Parameters
    ----------
    csv_path: Path
        Path to the CSV file to load.
    """

    if not csv_path.exists():
        print(f"✗ CSV not found: {csv_path}")
        sys.exit(1)

    table_fqn = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    print("\nLoading to BigQuery:")
    print(f"  From: {csv_path}")
    print(f"  To:   {table_fqn}")
    print()

    client = bigquery.Client(project=PROJECT_ID)

    # Explicit schema to avoid inference quirks with quoted/unquoted values
    schema = [
        bigquery.SchemaField("sample_id", "STRING"),
        bigquery.SchemaField("run_ids", "STRING"),
        bigquery.SchemaField("sample_name", "STRING"),
        bigquery.SchemaField("study_name", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=",",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",  # full refresh
        schema=schema,
        allow_quoted_newlines=True,
        encoding="UTF-8",
    )

    try:
        with open(csv_path, "rb") as f:
            load_job = client.load_table_from_file(f, table_fqn, job_config=job_config)

        print("Load job started, waiting for completion...")
        load_job.result()

        table = client.get_table(table_fqn)
        print("\n✓ Load complete!")
        print(f"  Table: {table_fqn}")
        print(f"  Rows:  {table.num_rows:,}")
        print(f"  Size:  {table.num_bytes / (1024**2):.2f} MB")
        print(f"  Created: {table.created}")
        return table

    except Exception as e:
        print(f"\n✗ Load failed: {e}")
        try:
            # If available, surface BigQuery job errors
            if 'load_job' in locals() and getattr(load_job, 'errors', None):
                print("\nLoad errors:")
                for error in load_job.errors:
                    print(f"  - {error}")
        except Exception:
            pass
        sys.exit(1)


def verify_table() -> None:
    """Run simple queries to verify the table was loaded correctly."""

    table_fqn = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    client = bigquery.Client(project=PROJECT_ID)

    print("\nVerifying table with test queries...")
    try:
        result = client.query(
            f"""
            SELECT
              COUNT(*) AS total_rows,
              COUNT(DISTINCT sample_id) AS distinct_sample_ids
            FROM `{table_fqn}`
            """
        ).result()

        for row in result:
            print(f"  Total rows:          {row.total_rows:,}")
            print(f"  Distinct sample_ids: {row.distinct_sample_ids:,}")

        print("\nSample rows:")
        sample = client.query(f"SELECT * FROM `{table_fqn}` LIMIT 3").result()
        for row in sample:
            print(f"  {dict(row)}")

        print("\n✓ Verification successful!")
    except Exception as e:
        print(f"✗ Verification query failed: {e}")


def main() -> None:
    print("=" * 80)
    print("Loading sample_id_map to BigQuery")
    print("=" * 80)
    print(f"Source: {LOCAL_CSV}")
    print(f"Target: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print("=" * 80)
    print()

    # Step 1: Load to BigQuery
    print("STEP 1: Load to BigQuery")
    print("-" * 80)
    load_to_bigquery(LOCAL_CSV)

    # Step 2: Verify
    print("\nSTEP 2: Verify Table")
    print("-" * 80)
    verify_table()

    print("\n" + "=" * 80)
    print("Load Complete!")
    print("=" * 80)
    print("\nYou can now query the table:")
    print(f"  SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` LIMIT 10")


if __name__ == "__main__":
    main()
