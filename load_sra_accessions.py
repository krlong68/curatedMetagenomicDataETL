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
from pathlib import Path
import httpx
from google.cloud import bigquery
from google.cloud import storage

# Configuration
PROJECT_ID = "curatedmetagenomicdata"
DATASET_ID = "curatedmetagenomicsdata"
TABLE_ID = "sra_accessions"
GCS_BUCKET = "cmgd-data"
GCS_PATH = "sra_metadata/SRA_Accessions.tab.gz"
SOURCE_URL = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab"
CHUNK_SIZE = 10 * 1024 * 1024  # 10MB chunks


def download_to_local(local_path):
    """Download file from NCBI to local filesystem with progress tracking."""

    print(f"Downloading to local file:")
    print(f"  From: {SOURCE_URL}")
    print(f"  To:   {local_path}")
    print("This may take 10-20 minutes for a 28GB file...\n")

    try:
        with httpx.stream("GET", SOURCE_URL, timeout=None, follow_redirects=True) as response:
            response.raise_for_status()

            # Get total size
            total_size = response.headers.get("Content-Length")
            if total_size:
                total_mb = int(total_size) / (1024**2)
                print(f"File size: {total_mb:.1f} MB ({int(total_size):,} bytes)")
            else:
                print("File size: Unknown")

            print("Downloading...")

            bytes_downloaded = 0
            chunk_num = 0

            with open(local_path, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=CHUNK_SIZE):
                    f.write(chunk)
                    bytes_downloaded += len(chunk)
                    chunk_num += 1

                    # Progress indicator every 100MB
                    if chunk_num % 10 == 0:
                        mb_downloaded = bytes_downloaded / (1024**2)
                        if total_size:
                            percent = (bytes_downloaded / int(total_size)) * 100
                            print(f"  Progress: {mb_downloaded:.1f} MB ({percent:.1f}%)")
                        else:
                            print(f"  Progress: {mb_downloaded:.1f} MB")

        print(f"\n✓ Download complete: {bytes_downloaded / (1024**3):.2f} GB")
        return bytes_downloaded

    except httpx.HTTPError as e:
        print(f"✗ HTTP Error during download: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error during download: {e}")
        sys.exit(1)


def compress_file(input_path, output_path):
    """Compress file with gzip."""

    print(f"\nCompressing file:")
    print(f"  From: {input_path}")
    print(f"  To:   {output_path}")

    try:
        bytes_read = 0
        bytes_written = 0
        chunk_num = 0

        with open(input_path, "rb") as f_in:
            with gzip.open(output_path, "wb", compresslevel=6) as f_out:
                while True:
                    chunk = f_in.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    f_out.write(chunk)
                    bytes_read += len(chunk)
                    chunk_num += 1

                    # Progress indicator every 500MB
                    if chunk_num % 50 == 0:
                        mb_read = bytes_read / (1024**2)
                        print(f"  Progress: {mb_read:.1f} MB compressed")

        # Get compressed size
        bytes_written = Path(output_path).stat().st_size

        print(f"\n✓ Compression complete:")
        print(f"  Original:   {bytes_read / (1024**3):.2f} GB")
        print(f"  Compressed: {bytes_written / (1024**3):.2f} GB")
        print(f"  Ratio:      {100 * bytes_written / bytes_read:.1f}%")

        return bytes_written

    except Exception as e:
        print(f"✗ Compression failed: {e}")
        sys.exit(1)


def upload_to_gcs(local_path, gcs_uri):
    """Upload local file to GCS."""

    print(f"\nUploading to GCS:")
    print(f"  From: {local_path}")
    print(f"  To:   {gcs_uri}")

    try:
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(GCS_PATH)

        # Upload with progress tracking
        blob.upload_from_filename(local_path)

        print(f"✓ Upload complete: {gcs_uri}")
        return gcs_uri

    except Exception as e:
        print(f"✗ Upload to GCS failed: {e}")
        sys.exit(1)


def load_to_bigquery(gcs_uri):
    """Load the gzipped file from GCS into BigQuery."""

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    print(f"\nLoading to BigQuery:")
    print(f"  From: {gcs_uri}")
    print(f"  To:   {table_id}")
    print("This will take several minutes...\n")

    client = bigquery.Client(project=PROJECT_ID)

    # Configure load job for gzipped TSV
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter='\t',
        autodetect=True,  # Let BigQuery infer schema
        write_disposition='WRITE_TRUNCATE',  # Full refresh
        skip_leading_rows=1,  # Skip header row
        allow_quoted_newlines=True,
        allow_jagged_rows=False,
        compression='GZIP',  # Handle gzipped input
    )

    # Start load job
    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
    )

    # Wait for completion
    print("Load job started, waiting for completion...")
    try:
        load_job.result()  # Wait for job to complete

        # Get table info
        table = client.get_table(table_id)
        print(f"\n✓ Load complete!")
        print(f"  Table: {table_id}")
        print(f"  Rows: {table.num_rows:,}")
        print(f"  Size: {table.num_bytes / (1024**3):.2f} GB")
        print(f"  Created: {table.created}")

        return table

    except Exception as e:
        print(f"\n✗ Load failed: {e}")
        # Show load errors if available
        if load_job.errors:
            print("\nLoad errors:")
            for error in load_job.errors:
                print(f"  - {error}")
        sys.exit(1)


def verify_table():
    """Run a simple query to verify the table works."""

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    print(f"\nVerifying table with test query...")

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


def cleanup_gcs(gcs_uri, keep_file=False):
    """Optionally clean up GCS file after load."""

    if keep_file:
        print(f"\nKeeping GCS file for future reloads: {gcs_uri}")
        print("  To delete manually: gcloud storage rm " + gcs_uri)
    else:
        print(f"\nCleaning up GCS file: {gcs_uri}")
        try:
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.bucket(GCS_BUCKET)
            blob = bucket.blob(GCS_PATH)
            blob.delete()
            print("✓ GCS file deleted")
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
    print("Loading SRA Accessions to BigQuery")
    print("="*80)
    print(f"Source: {SOURCE_URL}")
    print(f"Target: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print(f"Staging: gs://{GCS_BUCKET}/{GCS_PATH}")
    print("="*80)
    print()

    # Create temp directory for downloads
    temp_dir = tempfile.mkdtemp(prefix="sra_accessions_")
    print(f"Using temp directory: {temp_dir}\n")

    try:
        # Step 1: Download to local filesystem
        print("STEP 1: Download to Local Filesystem")
        print("-" * 80)
        local_file = Path(temp_dir) / "SRA_Accessions.tab"
        download_to_local(local_file)

        # Step 2: Compress with gzip
        print("\nSTEP 2: Compress with gzip")
        print("-" * 80)
        compressed_file = Path(temp_dir) / "SRA_Accessions.tab.gz"
        compress_file(local_file, compressed_file)

        # Step 3: Upload to GCS
        print("\nSTEP 3: Upload to GCS")
        print("-" * 80)
        gcs_uri = f"gs://{GCS_BUCKET}/{GCS_PATH}"
        upload_to_gcs(compressed_file, gcs_uri)

        # Step 4: Load to BigQuery
        print("\nSTEP 4: Load to BigQuery")
        print("-" * 80)
        table = load_to_bigquery(gcs_uri)

        # Step 5: Verify
        print("\nSTEP 5: Verify Table")
        print("-" * 80)
        verify_table()

        # Step 6: Cleanup GCS (keep by default since it's compressed)
        print("\nSTEP 6: Cleanup GCS")
        print("-" * 80)
        cleanup_gcs(gcs_uri, keep_file=False)

        # Step 7: Cleanup local files (always delete)
        print("\nSTEP 7: Cleanup Local Files")
        print("-" * 80)
        cleanup_local_files(temp_dir)

        print("\n" + "="*80)
        print("Load Complete!")
        print("="*80)
        print(f"\nYou can now query the table:")
        print(f"  SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` LIMIT 10")

    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        print("\nCleaning up temp files...")
        cleanup_local_files(temp_dir)
        sys.exit(1)


if __name__ == "__main__":
    main()
