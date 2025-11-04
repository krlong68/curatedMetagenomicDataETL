# curatedMetagenomicDataETL Pipeline
#
# Run `just --list` to see all available commands
# Run `just setup` to run the entire pipeline from scratch

# Default recipe - show help
default:
    @just --list

# =============================================================================
# Full Pipeline (run everything in order)
# =============================================================================

# Run the complete ETL pipeline from scratch
[group('pipeline')]
setup: create-external-tables load-reference-tables create-src-stg-tables gather-metadata
    @echo ""
    @echo "=========================================="
    @echo "✓ Complete ETL Pipeline Finished!"
    @echo "=========================================="
    @echo ""
    @echo "Next steps:"
    @echo "  - Review table_metadata.json for table statistics"
    @echo "  - See USER_GUIDE.md for query examples"

# =============================================================================
# Individual Pipeline Steps
# =============================================================================

# Step 1: Create BigQuery external tables pointing to GCS files
[group('steps')]
create-external-tables:
    @echo "=========================================="
    @echo "Step 1: Creating External Tables"
    @echo "=========================================="
    uv run create_external_tables.py

# Step 2: Load reference/lookup tables (sample_id_map, sra_accessions)
[group('steps')]
load-reference-tables: load-sample-id-map load-sra-accessions

# Step 2a: Load sample_id_map lookup table
[group('steps')]
load-sample-id-map:
    @echo "=========================================="
    @echo "Step 2a: Loading sample_id_map"
    @echo "=========================================="
    uv run load_sample_id_map_to_bigquery.py

# Step 2b: Load SRA accessions metadata (28GB download, takes 20-30 min)
[group('steps')]
load-sra-accessions:
    @echo "=========================================="
    @echo "Step 2b: Loading SRA Accessions"
    @echo "=========================================="
    @echo "WARNING: This will download, compress, and upload a 28GB file"
    @echo "         Estimated time: 20-30 minutes"
    @echo ""
    uv run load_sra_accessions.py

# Step 3: Create source views and staging tables (takes 30-60 min)
[group('steps')]
create-src-stg-tables:
    @echo "=========================================="
    @echo "Step 3: Creating Source Views and Staging Tables"
    @echo "=========================================="
    @echo "WARNING: This materializes 3.7B+ rows across 5 tables"
    @echo "         Estimated time: 30-60 minutes"
    @echo ""
    uv run create_src_stg_tables.py

# Step 4: Gather metadata about all tables and save to JSON
[group('steps')]
gather-metadata:
    @echo "=========================================="
    @echo "Step 4: Gathering Table Metadata"
    @echo "=========================================="
    uv run gather_table_metadata.py

# =============================================================================
# Incremental/Maintenance Tasks
# =============================================================================

# Refresh only staging tables (keeps external tables and reference data)
[group('maintenance')]
refresh-staging: create-src-stg-tables gather-metadata
    @echo "✓ Staging tables refreshed"

# Update only reference tables (sample_id_map and SRA accessions)
[group('maintenance')]
refresh-reference: load-reference-tables
    @echo "✓ Reference tables updated"

# Regenerate metadata without rebuilding tables
[group('maintenance')]
refresh-metadata: gather-metadata
    @echo "✓ Metadata refreshed"

# =============================================================================
# Validation and Testing
# =============================================================================

# Quick test queries to verify tables are working
[group('validation')]
test-tables:
    @echo "Running test queries on all tables..."
    @echo ""
    @echo "1. Testing ext_marker_abundance..."
    bq query --use_legacy_sql=false --max_rows=3 \
        'SELECT _FILE_NAME, marker_id, abundance FROM `curatedmetagenomicdata.curatedmetagenomicsdata.ext_marker_abundance` LIMIT 3'
    @echo ""
    @echo "2. Testing src_sample_id_map..."
    bq query --use_legacy_sql=false --max_rows=3 \
        'SELECT * FROM `curatedmetagenomicdata.curatedmetagenomicsdata.src_sample_id_map` LIMIT 3'
    @echo ""
    @echo "3. Testing stg_marker_abundance..."
    bq query --use_legacy_sql=false --max_rows=3 \
        'SELECT sample_id, marker_id, abundance FROM `curatedmetagenomicdata.curatedmetagenomicsdata.stg_marker_abundance` LIMIT 3'
    @echo ""
    @echo "✓ All test queries passed"

# Show current table sizes and row counts
[group('validation')]
show-tables:
    @echo "Current BigQuery tables:"
    @echo ""
    bq ls --format=pretty --max_results=50 curatedmetagenomicdata:curatedmetagenomicsdata

# =============================================================================
# Cleanup Tasks
# =============================================================================

# Remove all external tables (keeps data in GCS)
[group('cleanup')]
drop-external-tables:
    @echo "Dropping external tables (data in GCS will remain)..."
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.ext_marker_abundance
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.ext_marker_presence
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.ext_marker_rel_ab_w_read_stats
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.ext_metaphlan_unknown_list
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.ext_metaphlan_viruses_list
    @echo "✓ External tables dropped"

# Remove all source views
[group('cleanup')]
drop-source-views:
    @echo "Dropping source views..."
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.src_marker_abundance
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.src_marker_presence
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.src_marker_rel_ab_w_read_stats
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.src_metaphlan_unknown_list
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.src_metaphlan_viruses_list
    @echo "✓ Source views dropped"

# Remove all staging tables (WARNING: these are large materialized tables)
[group('cleanup')]
drop-staging-tables:
    @echo "WARNING: This will delete large materialized tables (3.7B+ rows)"
    @echo "Press Ctrl+C to cancel, or Enter to continue..."
    @read -r
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.stg_marker_abundance
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.stg_marker_presence
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.stg_marker_rel_ab_w_read_stats
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.stg_metaphlan_unknown_list
    bq rm -f -t curatedmetagenomicdata:curatedmetagenomicsdata.stg_metaphlan_viruses_list
    @echo "✓ Staging tables dropped"

# =============================================================================
# Development Helpers
# =============================================================================

# Install/update Python dependencies
[group('dev')]
install-deps:
    uv sync

# Run a simple query to test BigQuery access
[group('dev')]
test-bq-access:
    @echo "Testing BigQuery access..."
    bq query --use_legacy_sql=false "SELECT 1 as test"
    @echo "✓ BigQuery access confirmed"

# Show GCS bucket contents
[group('dev')]
show-gcs-files:
    @echo "Files in GCS bucket (first 20):"
    gcloud storage ls gs://cmgd-data/results/cMDv4/ | head -20

# Open BigQuery console in browser
[group('dev')]
open-bq-console:
    @echo "Opening BigQuery console..."
    open "https://console.cloud.google.com/bigquery?project=curatedmetagenomicdata&d=curatedmetagenomicsdata"
