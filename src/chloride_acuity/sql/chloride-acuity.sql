# starting with conductance readings over 500

select
    CosmoTimestamp,
    MAX(CASE WHEN CharacteristicName = "Specific conductance" THEN ResultValue END) AS Specific_Conductance,
    MAX(CASE WHEN CharacteristicName = "Temperature water" THEN ResultValue END) AS Temperature_Water,
    MAX(CASE WHEN CharacteristicName = "Water level (probe)" THEN ResultValue END) AS Water_Level
    from (
        WITH q1 AS (
            SELECT
                CAST(CONCAT(NSSK_COSMO.WAGG01.ActivityStartDate, ' ', NSSK_COSMO.WAGG01.ActivityStartTime) AS DATETIME) AS CosmoTimestamp,
                CharacteristicName,
                ResultValue
            FROM NSSK_COSMO.WAGG01
#            WHERE
#			CAST(CONCAT_WS(' ', NSSK_COSMO.$COSMO_SITE.ActivityStartDate, NSSK_COSMO.$COSMO_SITE.ActivityStartTime) as DATETIME) >= '$COSMO_START_DATETIME' AND
#			CAST(CONCAT_WS(' ', NSSK_COSMO.$COSMO_SITE.ActivityStartDate, NSSK_COSMO.$COSMO_SITE.ActivityStartTime) as DATETIME) < '$COSMO_END_DATETIME'
        )
        select
        CosmoTimestamp,
        CharacteristicName,
        ResultValue
        from q1
        WHERE
        (CharacteristicName = "Specific conductance" or CharacteristicName = "Temperature water" or CharacteristicName = "Water level (probe)")
    ) q2
    group by CosmoTimestamp
    having Specific_Conductance is not null and Specific_Conductance > 500;