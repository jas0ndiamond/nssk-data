-- one-off checks for data quality

-- cosmo

-- cosmo measurements
select DISTINCT NSSK_COSMO.WAGG01.CharacteristicName FROM NSSK_COSMO.WAGG01;
select DISTINCT NSSK_COSMO.WAGG03.CharacteristicName FROM NSSK_COSMO.WAGG03;

-- cosmo date ranges
select CAST(CONCAT_WS(' ', NSSK_COSMO.WAGG01.ActivityStartDate, NSSK_COSMO.WAGG01.ActivityStartTime) as DATETIME) as "COSMO_TIMESTAMP"
from NSSK_COSMO.WAGG01
order by COSMO_TIMESTAMP ASC
limit 5;

select CAST(CONCAT_WS(' ', NSSK_COSMO.WAGG01.ActivityStartDate, NSSK_COSMO.WAGG01.ActivityStartTime) as DATETIME) as "COSMO_TIMESTAMP"
from NSSK_COSMO.WAGG01
order by COSMO_TIMESTAMP DESC
limit 5;

select CAST(CONCAT_WS(' ', NSSK_COSMO.WAGG03.ActivityStartDate, NSSK_COSMO.WAGG03.ActivityStartTime) as DATETIME) as "COSMO_TIMESTAMP"
from NSSK_COSMO.WAGG03
order by COSMO_TIMESTAMP ASC
limit 5;

select CAST(CONCAT_WS(' ', NSSK_COSMO.WAGG03.ActivityStartDate, NSSK_COSMO.WAGG03.ActivityStartTime) as DATETIME) as "COSMO_TIMESTAMP"
from NSSK_COSMO.WAGG03
order by COSMO_TIMESTAMP DESC
limit 5;

-- cosmo time formats
select DISTINCT NSSK_COSMO.WAGG01.ActivityStartTime
from NSSK_COSMO.WAGG01
order by NSSK_COSMO.WAGG01.ActivityStartTime ASC;

select DISTINCT NSSK_COSMO.WAGG03.ActivityStartTime
from NSSK_COSMO.WAGG03
order by NSSK_COSMO.WAGG03.ActivityStartTime ASC;


-- bad water readings
SELECT *
FROM NSSK_COSMO.WAGG01
WHERE NSSK_COSMO.WAGG01.CharacteristicName = "Temperature water" AND (NSSK_COSMO.WAGG01.ResultValue < 0 OR NSSK_COSMO.WAGG01.ResultValue > 25);

SELECT *
FROM NSSK_COSMO.WAGG03
WHERE NSSK_COSMO.WAGG03.CharacteristicName = "Temperature water" AND (NSSK_COSMO.WAGG03.ResultValue < 0 OR NSSK_COSMO.WAGG03.ResultValue > 25);

-- bad conductivity readings
SELECT *
FROM NSSK_COSMO.WAGG01
WHERE NSSK_COSMO.WAGG01.CharacteristicName = "Conductivity" AND NSSK_COSMO.WAGG01.ResultValue < 0;

SELECT *
FROM NSSK_COSMO.WAGG01
WHERE NSSK_COSMO.WAGG01.CharacteristicName = "Specific conductance" AND NSSK_COSMO.WAGG01.ResultValue < 0;

SELECT *
FROM NSSK_COSMO.WAGG03
WHERE NSSK_COSMO.WAGG03.CharacteristicName = "Conductivity" AND NSSK_COSMO.WAGG03.ResultValue < 0;

SELECT *
FROM NSSK_COSMO.WAGG03
WHERE NSSK_COSMO.WAGG03.CharacteristicName = "Specific conductance" AND NSSK_COSMO.WAGG03.ResultValue < 0;

-- wagg01 water level
SELECT *
FROM NSSK_COSMO.WAGG01
WHERE NSSK_COSMO.WAGG01.CharacteristicName = "Water level (probe)" AND (NSSK_COSMO.WAGG01.ResultValue < 10 OR NSSK_COSMO.WAGG01.ResultValue > 11.5)
order by NSSK_COSMO.WAGG01.ResultValue ASC;

SELECT *
FROM NSSK_COSMO.WAGG01
WHERE NSSK_COSMO.WAGG01.CharacteristicName = "Water level (probe)" AND (NSSK_COSMO.WAGG01.ResultValue < 10 OR NSSK_COSMO.WAGG01.ResultValue > 11.5)
order by NSSK_COSMO.WAGG01.ResultValue DESC;

SELECT *
FROM NSSK_COSMO.WAGG01
WHERE NSSK_COSMO.WAGG01.CharacteristicName = "Water level (probe)"
order by NSSK_COSMO.WAGG01.ResultValue DESC;

SELECT Count(*)
FROM NSSK_COSMO.WAGG01;

SELECT Count(*)
FROM NSSK_COSMO.WAGG01
WHERE NSSK_COSMO.WAGG01.CharacteristicName = "Water level (probe)" AND (NSSK_COSMO.WAGG01.ResultValue < 10 OR NSSK_COSMO.WAGG01.ResultValue > 11.5);


-- cnv rainfall

-- air temperature
SELECT *
from NSSK_CNV_RAINFALL.CNV
where NSSK_CNV_RAINFALL.CNV.AirTemperature < -20 or NSSK_CNV_RAINFALL.CNV.AirTemperature > 40;

-- rainfall
SELECT *
from NSSK_CNV_RAINFALL.CNV
where NSSK_CNV_RAINFALL.CNV.HourlyRainfall < 0 or NSSK_CNV_RAINFALL.CNV.Rainfall < 0;

-- barometric pressure
SELECT *
from NSSK_CNV_RAINFALL.CNV
where NSSK_CNV_RAINFALL.CNV.BarometricPressure < 940 or NSSK_CNV_RAINFALL.CNV.BarometricPressure > 1060;

-- dnv flowworks

-- date ranges

select *
from NSSK_FLOWWORKS.DNV
order by NSSK_FLOWWORKS.DNV.MeasurementTimestamp ASC
limit 5;

select *
from NSSK_FLOWWORKS.DNV
order by NSSK_FLOWWORKS.DNV.MeasurementTimestamp DESC
limit 5;

-- flow range
select count(*)
from NSSK_FLOWWORKS.DNV
where NSSK_FLOWWORKS.DNV.FlowReading < 0 OR NSSK_FLOWWORKS.DNV.FlowReading > 100
order by NSSK_FLOWWORKS.DNV.FlowReading ASC;

select *
from NSSK_FLOWWORKS.DNV
where NSSK_FLOWWORKS.DNV.FlowReading < 0 OR NSSK_FLOWWORKS.DNV.FlowReading > 100
order by NSSK_FLOWWORKS.DNV.FlowReading ASC;
