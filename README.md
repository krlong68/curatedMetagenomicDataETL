# curatedMetagenomicDataETL

ETL pipeline for the [curatedMetagenomicData](https://github.com/waldronlab/curatedMetagenomicData) project, transforming 60,000+ per-sample metagenomic profiles into queryable BigQuery tables and distributable Parquet files.

## Quick Start

```bash
# Install dependencies
uv sync

# Run the complete ETL pipeline
just setup

# See all available commands
just --list
```

**⏱️ Estimated time:** 1-2 hours for complete pipeline (mostly unattended)

## Documentation

- **[USER_GUIDE.md](./USER_GUIDE.md)** - Query BigQuery tables, setup instructions, examples (for data users)
- **[CLAUDE.md](./CLAUDE.md)** - Pipeline architecture and execution guide (for developers/maintainers)
- **[table_metadata.json](./table_metadata.json)** - Current table statistics (auto-generated)
- **[justfile](./justfile)** - All available pipeline commands with descriptions

## Project Overview

This ETL pipeline processes metagenomic samples from the [curatedMetagenomicsNextflow](https://github.com/seandavi/curatedMetagenomicsNextflow) pipeline, combining per-sample TSV files into unified BigQuery tables for efficient querying and analysis.

**Pipeline Flow:**
1. **Source:** Per-sample files in GCS (`gs://cmgd-data/results/cMDv4/{sample_id}/`)
2. **Transform:** Combine samples into BigQuery tables (ext → src → stg layers)
3. **Output:** Query-optimized tables + Parquet exports for distribution

**Data Sources:**
- Raw sample files: [curatedMetagenomicsNextflow](https://github.com/seandavi/curatedMetagenomicsNextflow)
- Curated metadata: [curatedMetagenomicDataCuration](https://github.com/waldronlab/curatedMetagenomicDataCuration)

## Architecture

The pipeline uses a 3-layer design:

1. **External Tables (ext_*)** - Point to raw GCS files, no data movement
2. **Source Views (src_*)** - Add sample_id extraction, lightweight transformations
3. **Staging Tables (stg_*)** - Materialized, typed, clustered for production queries

**Reference Tables:**
- `src_sample_id_map` - Maps MD5 sample_id to SRA accessions and study names
- `sra_accessions` - Full NCBI SRA metadata

See [CLAUDE.md](./CLAUDE.md) for detailed architecture and [table_metadata.json](./table_metadata.json) for current table statistics.

## Technologies

- **Python 3.11+** with [uv](https://github.com/astral-sh/uv) package manager
- **BigQuery** for data warehouse and initial querying
- **DuckDB** for local analytics
- **Parquet** for columnar distribution format
- **just** for task orchestration ([justfile](https://github.com/casey/just))

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details on how to get started.

- **[CONTRIBUTING.md](CONTRIBUTING.md)** - Guidelines for contributing to the project
- **[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)** - Our community standards and expectations

This is part of the curatedMetagenomicData project providing uniformly processed metagenomic datasets to the scientific community. The data is derived from publicly available metagenomic sequencing studies processed using standardized pipelines for consistency and comparability.
