create or replace TABLE RAW_DB_INDIA.COMMONS_VP.AR_INTERNAL_METADATA (
	RAW_DATA VARIANT,
	_ETL_BATCH_ID VARCHAR(16777216),
	_ETL_INSERT_DATE_TIME TIMESTAMP_NTZ(9),
	_ETL_UPDATE_DATE_TIME TIMESTAMP_NTZ(9)
);