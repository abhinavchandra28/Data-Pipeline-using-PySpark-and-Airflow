
ETL pipeline using PySpark + Airflow is:
✅ Modular – Separate extract, transform, load logic
✅ Scalable – Uses PySpark for big data processing
✅ Automated – Managed with Airflow DAGs
✅ Version-Controlled – Organized in GitHub

This pipeline is a basic outline that can be used to build your first pipeline from scratch. Here's how to use the given scripts or modules:
-- The etl.py script has all the extract, transform and load functions that can be used to extract data by replacing the Database url, table name etc.
-- The transform function can be used for any number of complex transformations.
-- Load function would write the final transformed data to a postgreSQL table
