CREATE EXTERNAL TABLE IF NOT EXISTS trainer (
    sensorReadingTime STRING,
    serialNumber STRING,
    distanceFromObject FLOAT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://vineethabuckets/trainer/';

TBLPROPERTIES ('classification' = 'json');