#!/usr/bin/env python3
"""
Load SRA Accessions metadata from NCBI FTP to BigQuery.

Downloads the SRA_Accessions.tab file (28GB) and loads it into BigQuery.
Uses local temp file → gzip → GCS → BigQuery pipeline for reliability.
"""

import sys
import gzip
import shutil
import tempfile
import subprocess
from pathlib import Path
import httpx
from tqdm import tqdm
from google.cloud import bigquery
import duckdb

# Configuration
PROJECT_ID = "curatedmetagenomicdata"
DATASET_ID = "curatedmetagenomicsdata"
TABLE_ID = "src_sra_accessions"
GCS_BUCKET = "cmgd-data"
GCS_PATH_PREFIX = "sra_metadata/chunks/SRA_Accessions"  # Will add _part001.tab.gz, etc.
SOURCE_URL = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab"
CHUNK_SIZE = 10 * 1024 * 1024  # 10MB chunks for streaming
LINES_PER_CHUNK = 500000  # Split file into chunks of 500k lines each


def ncbi_to_parquet(input_tab_path, output_parquet_path):
    """Convert NCBI SRA Accessions tab file to Parquet using DuckDB for efficiency.
    
    Args:
        input_tab_path: Path to input .tab file
        output_parquet_path: Path to output .parquet file
    """

    print("\nConverting to Parquet using DuckDB:")
    print(f"  From: {input_tab_path}")
    print(f"  To:   {output_parquet_path}")

    try:
        con = duckdb.connect(database=':memory:')
        con.execute(f"""
            COPY (
                SELECT *
                FROM read_csv_auto('{input_tab_path}', delim='\t', header=True, nullstr='-')
            ) TO '{output_parquet_path}' (FORMAT 'parquet', COMPRESSION 'snappy', file_size_bytes '128MB', FILENAME_PATTERN 'sra_accessions_{{i}}');
        """)
        con.close()

        print("✓ Conversion to Parquet complete")

    except Exception as e:
        print(f"✗ Conversion to Parquet failed: {e}")
        sys.exit(1)


def upload_chunks_parallel(chunks_dir):
    """Upload all compressed chunk files to GCS in parallel using gcloud CLI.
    
    Uses wildcard pattern to upload all chunks with a single gcloud command,
    leveraging parallel composite uploads for maximum throughput.
    
    Args:
        chunks_dir: Directory containing the compressed chunk files (*.parquet)
    
    Returns:
        Wildcard GCS URI pattern for the uploaded files
    """
    
    chunks_dir = Path(chunks_dir)
    wildcard_pattern = str(chunks_dir / "*.parquet")
    gcs_destination = f"gs://{GCS_BUCKET}/{GCS_PATH_PREFIX.rsplit('/', 1)[0]}/"
    
    print("\nUploading chunks to GCS (parallel):")
    print(f"  From: {wildcard_pattern}")
    print(f"  To:   {gcs_destination}")
    
    try:
        # Use gcloud with wildcard for parallel uploads
        cmd = [
            "gcloud", "storage", "cp",
            wildcard_pattern,
            gcs_destination,
            "--recursive"
        ]
        
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        # Show gcloud output
        if result.stdout:
            print(result.stdout)
        
        # Return wildcard URI pattern for BigQuery load
        wildcard_uri = f"gs://{GCS_BUCKET}/{GCS_PATH_PREFIX}_part*.tab.gz"
        print("\n✓ Parallel upload complete")
        print(f"  URI pattern: {wildcard_uri}")
        
        return wildcard_uri
        
    except subprocess.CalledProcessError as e:
        print(f"✗ gcloud upload failed: {e}")
        if e.stderr:
            print(f"Error output: {e.stderr}")
        sys.exit(1)


def upload_to_gcs(local_path, gcs_uri):
    """Upload local file to GCS using gcloud CLI for faster parallel uploads."""

    print("\nUploading to GCS (using parallel composite upload):")
    print(f"  From: {local_path}")
    print(f"  To:   {gcs_uri}")

    try:
        # Use gcloud storage cp with parallel composite uploads (much faster)
        # This automatically splits the file into chunks and uploads them in parallel
        subprocess.run(
            ["gcloud", "storage", "cp", str(local_path), gcs_uri],
            check=True,
            capture_output=True,
            text=True,
        )

        print(f"✓ Upload complete: {gcs_uri}")
        return gcs_uri

    except subprocess.CalledProcessError as e:
        print(f"✗ Upload to GCS failed: {e}")
        if e.stderr:
            print(f"  Error output: {e.stderr}")
        sys.exit(1)
    except FileNotFoundError:
        print("✗ gcloud CLI not found. Please install: https://cloud.google.com/sdk/docs/install")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Upload to GCS failed: {e}")
        sys.exit(1)


def load_to_bigquery(gcs_uri_wildcard: str):
    """Load gzipped chunk files from GCS into BigQuery using a single wildcard URI.

    This issues one load job with WRITE_TRUNCATE and relies on BigQuery's
    support for wildcard URIs to ingest all matching files.
    """

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    print("\nLoading to BigQuery:")
    print(f"  From (wildcard): {gcs_uri_wildcard}")
    print(f"  To:              {table_id}")
    print("This will take several minutes...\n")

    client = bigquery.Client(project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter='\t',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        allow_jagged_rows=False,
        compression='GZIP',
        null_marker='-',
    )

    # One load job with wildcard
    load_job = client.load_table_from_uri(
        gcs_uri_wildcard,
        table_id,
        job_config=job_config,
    )

    try:
        load_job.result()
        table = client.get_table(table_id)
        print("\n✓ Load complete!")
        print(f"  Table: {table_id}")
        print(f"  Rows:  {table.num_rows:,}")
        print(f"  Size:  {table.num_bytes / (1024**3):.2f} GB")
        print(f"  Created: {table.created}")
        return table
    except Exception as e:
        print(f"\n✗ Load failed: {e}")
        if load_job.errors:
            print("\nLoad errors:")
            for error in load_job.errors:
                print(f"  - {error}")
        sys.exit(1)


def verify_table():
    """Run a simple query to verify the table works."""

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    print("\nVerifying table with test query...")

    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT Accession) as unique_accessions
    FROM `{table_id}`
    """

    try:
        result = client.query(query).result()

        for row in result:
            print(f"  Total rows: {row.total_rows:,}")
            print(f"  Unique accessions: {row.unique_accessions:,}")

        print("\n✓ Verification successful!")

        # Show sample of the data
        print("\nSample rows:")
        sample_query = f"SELECT * FROM `{table_id}` LIMIT 3"
        sample_result = client.query(sample_query).result()

        for row in sample_result:
            print(f"  {dict(row)}")

    except Exception as e:
        print(f"✗ Verification query failed: {e}")


def cleanup_gcs(gcs_uris, keep_file=False):
    """Optionally clean up GCS files after load.
    
    Args:
        gcs_uris: Single URI string or list of URIs to clean up
    """

    # Handle both single file and list of files
    if isinstance(gcs_uris, str):
        gcs_uris = [gcs_uris]

    if keep_file:
        print(f"\nKeeping {len(gcs_uris)} GCS file(s) for future reloads")
        for uri in gcs_uris[:3]:
            print(f"  {uri}")
        if len(gcs_uris) > 3:
            print(f"  ... and {len(gcs_uris) - 3} more")
        print("  To delete manually: gcloud storage rm gs://BUCKET/path/to/files/*")
    else:
        print(f"\nCleaning up {len(gcs_uris)} GCS file(s)...")
        try:
            # Use gcloud to delete all chunk files efficiently
            if len(gcs_uris) > 0:
                # Extract the pattern for bulk delete
                subprocess.run(
                    ["gcloud", "storage", "rm"] + gcs_uris,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                print(f"✓ {len(gcs_uris)} GCS file(s) deleted")
        except subprocess.CalledProcessError as e:
            print(f"✗ Cleanup failed (non-critical): {e}")
            if e.stderr:
                print(f"  Error: {e.stderr}")
        except Exception as e:
            print(f"✗ Cleanup failed (non-critical): {e}")


def cleanup_local_files(temp_dir):
    """Clean up local temporary files."""

    print(f"\nCleaning up local temp files: {temp_dir}")
    try:
        shutil.rmtree(temp_dir)
        print("✓ Local temp files deleted")
    except Exception as e:
        print(f"✗ Local cleanup failed (non-critical): {e}")


def main():
    """Main function to orchestrate the load."""

    print("="*80)
    print("Loading SRA Accessions to BigQuery (Chunked)")
    print("="*80)
    print(f"Source: {SOURCE_URL}")
    print(f"Target: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print(f"Staging: gs://{GCS_BUCKET}/{GCS_PATH_PREFIX}_*.tab.gz")
    print(f"Chunk size: {LINES_PER_CHUNK:,} lines per file")
    print("="*80)
    print()

    # Create temp directory for downloads
    temp_dir = tempfile.mkdtemp(prefix="sra_accessions_")
    print(f"Using temp directory: {temp_dir}\n")

    try:

        # Step 0: Convert NCBI SRA Accessions tab file to Parquet
        print("STEP 0: Convert NCBI SRA Accessions to Parquet")
        print("-" * 80)
        ncbi_to_parquet(SOURCE_URL, Path(temp_dir))
        
        exit(2)

        # Step 2: Upload chunks to GCS (parallel)
        print("\nSTEP 2: Upload Chunks to GCS (Parallel)")
        print("-" * 80)
        wildcard_uri = upload_chunks_parallel(Path(temp_dir))

        # Step 3: Load to BigQuery (single job with wildcard)
        print("\nSTEP 3: Load to BigQuery")
        print("-" * 80)
        load_to_bigquery(wildcard_uri)

        # Step 4: Verify
        print("\nSTEP 4: Verify Table")
        print("-" * 80)
        verify_table()

        # Step 5: Cleanup GCS (keep by default since it's compressed)
        print("\nSTEP 5: Cleanup GCS")
        print("-" * 80)
        cleanup_gcs(wildcard_uri, keep_file=True)

        # Step 6: Cleanup local files (always delete)
        print("\nSTEP 6: Cleanup Local Files")
        print("-" * 80)
        cleanup_local_files(temp_dir)

        print("\n" + "="*80)
        print("Load Complete!")
        print("="*80)
        print("\nYou can now query the table:")
        print(f"  SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` LIMIT 10")

    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        print("\nCleaning up temp files...")
        ## cleanup_local_files(temp_dir)
        sys.exit(1)


if __name__ == "__main__":
    main()
