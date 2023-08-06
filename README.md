# python_dwh_etl
ETL job that can take any table and move it from one postgresql database to another with logging.
takes in table name, source schema, target schema and db config files for source and target db. Moves the data from source to target, full load if empty else incremental load.
