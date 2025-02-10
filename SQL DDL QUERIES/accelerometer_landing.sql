CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer (
    user STRING,
    timestamp STRING,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://vineethabuckets/accelerometer/';

TBLPROPERTIES ('classification' = 'json');