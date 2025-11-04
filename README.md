# curatedMetagenomicDataETL

This package contains the code and documentation for the ETL (Extract, Transform, Load) process used in the curatedMetagenomicData project. The scope of the ETL process is to take the csv/tsv files output PER SAMPLE from the [curatedMetagenomicsNextflow](https://github.com/seandavi/curatedMetagenomicsNextflow) pipeline and load them into a structured database and parquet files for easy access and analysis. 

Since the data are stored PER SAMPLE, the ETL process involves combining data from multiple samples into unified tables for each data type (e.g., taxonomic profiles, functional profiles, metadata). 

Technologies used in this ETL process include Python, duckdb, parquet, and BigQuery. Since there are over 60000 samples, the ETL process is designed to be efficient and scalable, leveraging parallel processing and optimized data storage formats. 

We plan to use BigQuery for the initial loading and querying of the data, and then export the final tables as parquet files for distribution on cloudflare R2 and further analysis using duckdb.

## Background

The curatedMetagenomicData project is a combinination of efforts to provide curated metadata and uniformly processed metagenomic data sets to the scientific community. The data sets are derived from publicly available metagenomic sequencing studies and are processed using standardized pipelines to ensure consistency and comparability across studies.

## Data Processing

The metagenomic data sets are processed using a series of bioinformatics tools and pipelines. The raw sequencing data is first quality-checked and filtered to remove low-quality reads. The filtered reads are then taxonomically classified and functionally annotated using established databases and algorithms. The resulting data is organized into a structured format, allowing for easy access and analysis.

### Data storage and Access

The curated metagenomic metadata are stored in csv files in a GitHub repository: [curatedMetagenomicData](https://github.com/waldronlab/curatedMetagenomicDataCuration

The processed data are computed using the [curatedMetagenomicsNextflow](https://github.com/seandavi/curatedMetagenomicsNextflow) pipeline. The processed data are stored in a google cloud storage bucket: `gs://cmgd-data/results/cMDv4/{sample_id}/`.

Files stored per sample include:

| File Name | Description |
| --------- | ----------- |
| `metaphlan_markers/marker_abundance.tsv.gz` | Taxonomic marker abundance profiles for the sample, two columns: marker identifier and abundance value. |
| `metaphlan_markers/marker_presence.tsv.gz` | Taxonomic marker presence/absence profiles for the sample, two columns: marker identifier and presence/absence value. |
| `metaphlan_markers/marker_rel_ab_w_read_stats.tsv.gz` | Taxonomic marker relative abundance with read statistics for the sample, multiple columns including marker identifier, relative abundance, and read statistics. |
| `metaphlan_lists/metaphlan_unknown_list.tsv.gz` | List of unknown taxa identified in the sample, two columns: taxon identifier and count of unknown reads. |
| `metaphlan_lists/metaphlan_viruses_list.tsv.gz` | List of viral taxa identified in the sample, two columns: taxon identifier and count of viral reads. |
| `strainphlan_markers/metaphlan.json.bz2` | strainphlan marker information for the sample in JSON format. |
