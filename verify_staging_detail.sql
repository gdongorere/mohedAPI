-- Staging HTS Verification
SELECT 
    Indicator,
    SUM(Value) as StagingCount
FROM IndicatorValues_Prevention
WHERE Indicator IN ('HTS_TST', 'HTS_POS', 'LINKAGE_ART')
AND VisitDate >= '2024-01-01'
GROUP BY Indicator;

-- Staging PrEP Verification
SELECT 
    Indicator,
    SUM(Value) as StagingCount
FROM IndicatorValues_Prevention
WHERE Indicator IN ('PREP_NEW', 'PREP_SEROCONVERSION')
AND VisitDate >= '2024-01-01'
GROUP BY Indicator;

-- Staging ART Verification
SELECT 
    Indicator,
    SUM(Value) as StagingCount
FROM IndicatorValues_HIV
WHERE Indicator IN ('TX_CURR', 'TX_VL_TESTED', 'TX_VL_SUPPRESSED')
AND VisitDate >= '2024-01-01'
GROUP BY Indicator;
