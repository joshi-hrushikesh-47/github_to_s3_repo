create or replace TABLE RAW_DB_INDIA.OMS_AGG_INTERMEDIATE.COMPLETED_SPOT_ORDERS_FOR_LTO_LTV_SOURCES (
	RAW_DATA VARIANT,
	_ETL_BATCH_ID VARCHAR(16777216),
	_ETL_INSERT_DATE_TIME TIMESTAMP_NTZ(9),
	_ETL_UPDATE_DATE_TIME TIMESTAMP_NTZ(9)
);
