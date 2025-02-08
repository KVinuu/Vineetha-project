CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
    customerName STRING,
    email STRING,
    phoneNumber STRING,
    birthDay STRING,
    serialNumber STRING,
    registrationDate STRING,
    lastUpdateDate STRING,
    shareWithResearchAsOfDate STRING,
    shareWithPublicAsOfDate STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://vineethabuckets/customer_landing/';
TBLPROPERTIES ('classification' = 'json');
