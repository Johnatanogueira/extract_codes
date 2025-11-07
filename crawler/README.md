## Data Crawler

This project provides two independent Python crawlers designed for automated data extraction, transformation, and loading (ETL) from configurable data sources.
The crawlers are responsible for downloading structured datasets, performing data cleaning and deduplication, and loading the results into AWS Athena tables in Parquet (Snappy) format on Amazon S3.

The entire environment is containerized using Docker, ensuring reproducibility, dependency isolation, and ease of deployment.
