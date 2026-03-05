-- HTS Detailed Verification
SELECT 
    'HTS_TST' as Metric,
    COUNT(*) as SourceCount,
    SUM(CASE WHEN HTS_TestedForHIV = 1 THEN 1 ELSE 0 END) as Tested,
    SUM(CASE WHEN HTS_TestedPositive = 1 THEN 1 ELSE 0 END) as Positive,
    SUM(CASE WHEN HTS_TestedPositiveInitiatedOnART = 1 THEN 1 ELSE 0 END) as Linked
FROM tmpHTSTestedDetail
WHERE VisitDate >= '2024-01-01';

-- PrEP Detailed Verification
SELECT 
    'PrEP_New' as Metric,
    COUNT(*) as SourceCount,
    SUM(CASE WHEN PrEP_Initiation = 1 THEN 1 ELSE 0 END) as Initiations
FROM LineListingsPrep
WHERE VisitDate >= '2024-01-01';

SELECT 
    'PrEP_Seroconversions' as Metric,
    COUNT(*) as SourceCount,
    SUM(CASE WHEN Seroconverted IN ('1', 'true', 'yes') THEN 1 ELSE 0 END) as Seroconversions
FROM aPrepDetail
WHERE VisitDate >= '2024-01-01';

-- ART Detailed Verification
SELECT 
    'ART_Current' as Metric,
    COUNT(*) as SourceCount,
    SUM(CASE WHEN TX_CURR = 1 THEN 1 ELSE 0 END) as OnART
FROM tmpARTTXOutcomes
WHERE ReportingPeriod >= '2024-01-01';
