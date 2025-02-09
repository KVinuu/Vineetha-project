CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing_to_trusted (
    user STRING,
    timestamp STRING,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://vineethabuckets/accelerometer_landing_to_trusted/';

TBLPROPERTIES ('classification' = 'json');