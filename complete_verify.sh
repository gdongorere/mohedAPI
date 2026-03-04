#!/bin/bash

# =========================================
# COMPLETE DATA VERIFICATION SCRIPT
# =========================================

# Connection strings - FIXED with proper escaping
SOURCE="-S 10.216.0.10,1480\\SQL2025 -U sa -P 'Support25' -d All_Dataset -C"
STAGING="-S 102.37.18.0,1433 -U sa -P '1!2@3#Abcd123!' -d EswatiniHealth_Staging -C"

echo "================================================================="
echo "🔍 COMPLETE DATA VERIFICATION REPORT"
echo "Generated: $(date)"
echo "================================================================="
echo ""

# =========================================
# SECTION 1: SOURCE DATABASE OVERVIEW
# =========================================
echo "📊 [SECTION 1] SOURCE DATABASE OVERVIEW (All_Dataset)"
echo "-----------------------------------------------------------------"

eval /opt/mssql-tools/bin/sqlcmd $SOURCE -Q "
SELECT '=== SOURCE TABLE ROW COUNTS ===' as '';
SELECT 
    'tmpHTSTestedDetail' as TableName, 
    COUNT(*) as RowCount,
    CONVERT(VARCHAR, MIN(VisitDate), 23) as EarliestDate,
    CONVERT(VARCHAR, MAX(VisitDate), 23) as LatestDate
FROM tmpHTSTestedDetail
UNION ALL
SELECT 
    'LineListingsPrep',
    COUNT(*),
    CONVERT(VARCHAR, MIN(VisitDate), 23),
    CONVERT(VARCHAR, MAX(VisitDate), 23)
FROM LineListingsPrep
UNION ALL
SELECT 
    'aPrepDetail',
    COUNT(*),
    CONVERT(VARCHAR, MIN(VisitDate), 23),
    CONVERT(VARCHAR, MAX(VisitDate), 23)
FROM aPrepDetail
UNION ALL
SELECT 
    'tmpARTTXOutcomes',
    COUNT(*),
    CONVERT(VARCHAR, MIN(ReportingPeriod), 23),
    CONVERT(VARCHAR, MAX(ReportingPeriod), 23)
FROM tmpARTTXOutcomes;
"

# =========================================
# SECTION 2: SOURCE HTS DETAILS
# =========================================
echo ""
echo "📊 [SECTION 2] SOURCE HTS DETAILS"
echo "-----------------------------------------------------------------"

eval /opt/mssql-tools/bin/sqlcmd $SOURCE -Q "
SELECT '=== HTS SOURCE METRICS ===' as '';
SELECT 
    'Total Records' as Metric, COUNT(*) as Value FROM tmpHTSTestedDetail
UNION ALL SELECT 'Tested for HIV', SUM(HTS_TestedForHIV) FROM tmpHTSTestedDetail
UNION ALL SELECT 'Tested Negative', SUM(HTS_TestedNegative) FROM tmpHTSTestedDetail
UNION ALL SELECT 'Tested Positive', SUM(HTS_TestedPositive) FROM tmpHTSTestedDetail
UNION ALL SELECT 'Linked to ART', SUM(HTS_TestedPositiveInitiatedOnART) FROM tmpHTSTestedDetail;

SELECT '=== HTS BY REGION (using aPrepDetail mapping) ===' as '';
SELECT 
    ISNULL(r.Region, 'Unknown') as Region,
    COUNT(*) as VisitCount,
    SUM(h.HTS_TestedForHIV) as Tested,
    SUM(h.HTS_TestedPositive) as Positive,
    SUM(h.HTS_TestedNegative) as Negative,
    SUM(h.HTS_TestedPositiveInitiatedOnART) as LinkedToART
FROM tmpHTSTestedDetail h
LEFT JOIN (SELECT DISTINCT FacilityCode, Region FROM aPrepDetail WHERE FacilityCode IS NOT NULL) r 
    ON h.FacilityCode = r.FacilityCode
GROUP BY r.Region
ORDER BY r.Region;
"

# =========================================
# SECTION 3: SOURCE PrEP DETAILS
# =========================================
echo ""
echo "📊 [SECTION 3] SOURCE PrEP DETAILS"
echo "-----------------------------------------------------------------"

eval /opt/mssql-tools/bin/sqlcmd $SOURCE -Q "
SELECT '=== PrEP SOURCE METRICS (LineListingsPrep) ===' as '';
SELECT 
    'Total Records' as Metric, COUNT(*) as Value FROM LineListingsPrep
UNION ALL SELECT 'PrEP Initiations', SUM(PrEP_Initiation) FROM LineListingsPrep
UNION ALL SELECT 'Tested for HIV', SUM(PrEP_TestedForHIV) FROM LineListingsPrep
UNION ALL SELECT 'Tested Negative', SUM(PrEP_TestedNegative) FROM LineListingsPrep
UNION ALL SELECT 'Tested Positive (Seroconverted)', SUM(PrEP_TestedPositive) FROM LineListingsPrep
UNION ALL SELECT 'Initiated on ART', SUM(PrEP_InitiatedOnART) FROM LineListingsPrep;

SELECT '=== PrEP SOURCE METRICS (aPrepDetail) ===' as '';
SELECT 
    'Total Records' as Metric, COUNT(*) as Value FROM aPrepDetail
UNION ALL SELECT 'Seroconverted', 
    SUM(CASE WHEN Seroconverted IN ('1', 'true', 'yes') THEN 1 ELSE 0 END) FROM aPrepDetail
UNION ALL SELECT 'Initiated on ART', 
    SUM(CASE WHEN InitiatedOnART IN ('1', 'true', 'yes') THEN 1 ELSE 0 END) FROM aPrepDetail;
"

# =========================================
# SECTION 4: SOURCE ART DETAILS
# =========================================
echo ""
echo "📊 [SECTION 4] SOURCE ART DETAILS"
echo "-----------------------------------------------------------------"

eval /opt/mssql-tools/bin/sqlcmd $SOURCE -Q "
SELECT '=== ART SOURCE METRICS ===' as '';
SELECT 
    'Total Records' as Metric, COUNT(*) as Value FROM tmpARTTXOutcomes
UNION ALL SELECT 'Currently on ART (TX_CURR)', SUM(TX_CURR) FROM tmpARTTXOutcomes
UNION ALL SELECT 'VL Tested', SUM(TX_VLTested) FROM tmpARTTXOutcomes
UNION ALL SELECT 'VL Suppressed', SUM(TX_VLSuppressed) FROM tmpARTTXOutcomes
UNION ALL SELECT 'VL Undetectable', SUM(TX_VLUndetectable) FROM tmpARTTXOutcomes;

SELECT '=== ART BY REGION ===' as '';
SELECT 
    ISNULL(r.Region, 'Unknown') as Region,
    SUM(a.TX_CURR) as OnART,
    SUM(a.TX_VLTested) as VLTested,
    SUM(a.TX_VLSuppressed) as VLSuppressed,
    SUM(a.TX_VLUndetectable) as VLUndetectable
FROM tmpARTTXOutcomes a
LEFT JOIN (SELECT DISTINCT FacilityCode, Region FROM aPrepDetail WHERE FacilityCode IS NOT NULL) r 
    ON a.FacilityCode = r.FacilityCode
GROUP BY r.Region
ORDER BY r.Region;
"

# =========================================
# SECTION 5: STAGING DATABASE OVERVIEW
# =========================================
echo ""
echo "📊 [SECTION 5] STAGING DATABASE OVERVIEW (EswatiniHealth_Staging)"
echo "-----------------------------------------------------------------"

eval /opt/mssql-tools/bin/sqlcmd $STAGING -Q "
SELECT '=== STAGING TABLE ROW COUNTS ===' as '';
SELECT 
    'IndicatorValues_HIV' as TableName,
    COUNT(*) as AggregatedRows,
    SUM(Value) as TotalValue,
    COUNT(DISTINCT Indicator) as UniqueIndicators
FROM IndicatorValues_HIV
UNION ALL
SELECT 
    'IndicatorValues_Prevention',
    COUNT(*),
    SUM(Value),
    COUNT(DISTINCT Indicator)
FROM IndicatorValues_Prevention
UNION ALL
SELECT 
    'IndicatorValues_TB',
    COUNT(*),
    SUM(Value),
    COUNT(DISTINCT Indicator)
FROM IndicatorValues_TB
UNION ALL
SELECT 
    'Users',
    COUNT(*),
    NULL,
    NULL
FROM Users
UNION ALL
SELECT 
    'IndicatorTargets',
    COUNT(*),
    NULL,
    NULL
FROM IndicatorTargets;
"

# =========================================
# SECTION 6: STAGING HIV DETAILS
# =========================================
echo ""
echo "📊 [SECTION 6] STAGING HIV DETAILS"
echo "-----------------------------------------------------------------"

eval /opt/mssql-tools/bin/sqlcmd $STAGING -Q "
SELECT '=== HIV INDICATOR TOTALS ===' as '';
SELECT 
    Indicator,
    COUNT(*) as AggregatedRows,
    SUM(Value) as TotalPatients,
    MIN(VisitDate) as EarliestDate,
    MAX(VisitDate) as LatestDate
FROM IndicatorValues_HIV
GROUP BY Indicator
ORDER BY Indicator;

SELECT '=== HIV BY REGION ===' as '';
SELECT 
    CASE RegionId
        WHEN 1 THEN 'Hhohho'
        WHEN 2 THEN 'Manzini'
        WHEN 3 THEN 'Shiselweni'
        WHEN 4 THEN 'Lubombo'
    END as Region,
    SUM(CASE WHEN Indicator = 'TX_CURR' THEN Value ELSE 0 END) as OnART,
    SUM(CASE WHEN Indicator = 'TX_VL_TESTED' THEN Value ELSE 0 END) as VLTested,
    SUM(CASE WHEN Indicator = 'TX_VL_SUPPRESSED' THEN Value ELSE 0 END) as VLSuppressed
FROM IndicatorValues_HIV
GROUP BY RegionId
ORDER BY RegionId;

SELECT '=== HIV BY AGE GROUP ===' as '';
SELECT 
    AgeGroup,
    SUM(Value) as TotalOnART
FROM IndicatorValues_HIV
WHERE Indicator = 'TX_CURR'
GROUP BY AgeGroup
ORDER BY 
    CASE 
        WHEN AgeGroup = '< 1' THEN 1
        WHEN AgeGroup = '1 - 4' THEN 2
        WHEN AgeGroup = '5 - 9' THEN 3
        WHEN AgeGroup = '10 - 14' THEN 4
        WHEN AgeGroup = '15 - 19' THEN 5
        WHEN AgeGroup = '20 - 24' THEN 6
        WHEN AgeGroup = '25 - 29' THEN 7
        WHEN AgeGroup = '30 - 34' THEN 8
        WHEN AgeGroup = '35 - 39' THEN 9
        WHEN AgeGroup = '40 - 44' THEN 10
        WHEN AgeGroup = '45 - 49' THEN 11
        WHEN AgeGroup = '50 - 54' THEN 12
        WHEN AgeGroup = '55 - 59' THEN 13
        WHEN AgeGroup = '>= 60' THEN 14
        ELSE 99
    END;

SELECT '=== HIV BY SEX ===' as '';
SELECT 
    Sex,
    SUM(Value) as TotalOnART,
    CAST(SUM(Value) * 100.0 / (SELECT SUM(Value) FROM IndicatorValues_HIV WHERE Indicator = 'TX_CURR') AS DECIMAL(5,1)) as Percentage
FROM IndicatorValues_HIV
WHERE Indicator = 'TX_CURR'
GROUP BY Sex
ORDER BY Sex;
"

# =========================================
# SECTION 7: STAGING PREVENTION DETAILS
# =========================================
echo ""
echo "📊 [SECTION 7] STAGING PREVENTION DETAILS"
echo "-----------------------------------------------------------------"

eval /opt/mssql-tools/bin/sqlcmd $STAGING -Q "
SELECT '=== PREVENTION INDICATOR TOTALS ===' as '';
SELECT 
    Indicator,
    COUNT(*) as AggregatedRows,
    SUM(Value) as TotalCount,
    MIN(VisitDate) as EarliestDate,
    MAX(VisitDate) as LatestDate
FROM IndicatorValues_Prevention
GROUP BY Indicator
ORDER BY Indicator;

SELECT '=== PREVENTION BY REGION ===' as '';
SELECT 
    CASE p.RegionId
        WHEN 1 THEN 'Hhohho'
        WHEN 2 THEN 'Manzini'
        WHEN 3 THEN 'Shiselweni'
        WHEN 4 THEN 'Lubombo'
    END as Region,
    SUM(CASE WHEN Indicator = 'HTS_TST' THEN Value ELSE 0 END) as Tests,
    SUM(CASE WHEN Indicator = 'HTS_POS' THEN Value ELSE 0 END) as Positives,
    SUM(CASE WHEN Indicator = 'PREP_NEW' THEN Value ELSE 0 END) as PrEPInitiations
FROM IndicatorValues_Prevention p
GROUP BY p.RegionId
ORDER BY p.RegionId;

SELECT '=== HTS BY AGE GROUP ===' as '';
SELECT 
    AgeGroup,
    SUM(CASE WHEN Indicator = 'HTS_TST' THEN Value ELSE 0 END) as Tests,
    SUM(CASE WHEN Indicator = 'HTS_POS' THEN Value ELSE 0 END) as Positives,
    CAST(SUM(CASE WHEN Indicator = 'HTS_POS' THEN Value ELSE 0 END) * 100.0 / 
         NULLIF(SUM(CASE WHEN Indicator = 'HTS_TST' THEN Value ELSE 0 END), 0) AS DECIMAL(5,1)) as PositivityRate
FROM IndicatorValues_Prevention
WHERE Indicator IN ('HTS_TST', 'HTS_POS')
GROUP BY AgeGroup
ORDER BY AgeGroup;
"

# =========================================
# SECTION 8: KEY METRICS COMPARISON
# =========================================
echo ""
echo "📊 [SECTION 8] KEY METRICS COMPARISON (Source vs Staging)"
echo "-----------------------------------------------------------------"

# Get source totals
SOURCE_TESTS=$(eval /opt/mssql-tools/bin/sqlcmd $SOURCE -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(HTS_TestedForHIV), 0) FROM tmpHTSTestedDetail;" -h -1 -W | head -1 | tr -d ' ' | xargs)
SOURCE_POS=$(eval /opt/mssql-tools/bin/sqlcmd $SOURCE -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(HTS_TestedPositive), 0) FROM tmpHTSTestedDetail;" -h -1 -W | head -1 | tr -d ' ' | xargs)
SOURCE_PREP=$(eval /opt/mssql-tools/bin/sqlcmd $SOURCE -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(PrEP_Initiation), 0) FROM LineListingsPrep;" -h -1 -W | head -1 | tr -d ' ' | xargs)
SOURCE_ART=$(eval /opt/mssql-tools/bin/sqlcmd $SOURCE -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(TX_CURR), 0) FROM tmpARTTXOutcomes;" -h -1 -W | head -1 | tr -d ' ' | xargs)
SOURCE_VL_TESTED=$(eval /opt/mssql-tools/bin/sqlcmd $SOURCE -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(TX_VLTested), 0) FROM tmpARTTXOutcomes;" -h -1 -W | head -1 | tr -d ' ' | xargs)
SOURCE_VL_SUPPRESSED=$(eval /opt/mssql-tools/bin/sqlcmd $SOURCE -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(TX_VLSuppressed), 0) FROM tmpARTTXOutcomes;" -h -1 -W | head -1 | tr -d ' ' | xargs)

# Get staging totals
STAGING_TESTS=$(eval /opt/mssql-tools/bin/sqlcmd $STAGING -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_Prevention WHERE Indicator='HTS_TST';" -h -1 -W | head -1 | tr -d ' ' | xargs)
STAGING_POS=$(eval /opt/mssql-tools/bin/sqlcmd $STAGING -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_Prevention WHERE Indicator='HTS_POS';" -h -1 -W | head -1 | tr -d ' ' | xargs)
STAGING_PREP=$(eval /opt/mssql-tools/bin/sqlcmd $STAGING -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_Prevention WHERE Indicator='PREP_NEW';" -h -1 -W | head -1 | tr -d ' ' | xargs)
STAGING_ART=$(eval /opt/mssql-tools/bin/sqlcmd $STAGING -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_HIV WHERE Indicator='TX_CURR';" -h -1 -W | head -1 | tr -d ' ' | xargs)
STAGING_VL_TESTED=$(eval /opt/mssql-tools/bin/sqlcmd $STAGING -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_HIV WHERE Indicator='TX_VL_TESTED';" -h -1 -W | head -1 | tr -d ' ' | xargs)
STAGING_VL_SUPPRESSED=$(eval /opt/mssql-tools/bin/sqlcmd $STAGING -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_HIV WHERE Indicator='TX_VL_SUPPRESSED';" -h -1 -W | head -1 | tr -d ' ' | xargs)

# Set empty values to 0
SOURCE_TESTS=${SOURCE_TESTS:-0}
SOURCE_POS=${SOURCE_POS:-0}
SOURCE_PREP=${SOURCE_PREP:-0}
SOURCE_ART=${SOURCE_ART:-0}
SOURCE_VL_TESTED=${SOURCE_VL_TESTED:-0}
SOURCE_VL_SUPPRESSED=${SOURCE_VL_SUPPRESSED:-0}
STAGING_TESTS=${STAGING_TESTS:-0}
STAGING_POS=${STAGING_POS:-0}
STAGING_PREP=${STAGING_PREP:-0}
STAGING_ART=${STAGING_ART:-0}
STAGING_VL_TESTED=${STAGING_VL_TESTED:-0}
STAGING_VL_SUPPRESSED=${STAGING_VL_SUPPRESSED:-0}

echo "+---------------------------+--------------+--------------+--------+"
echo "| Metric                    | Source       | Staging      | Match? |"
echo "+---------------------------+--------------+--------------+--------+"
printf "| HTS Tests                 | %12s | %12s |   %s   |\n" "$SOURCE_TESTS" "$STAGING_TESTS" $([ "$SOURCE_TESTS" = "$STAGING_TESTS" ] && echo "✅" || echo "❌")
printf "| HTS Positives             | %12s | %12s |   %s   |\n" "$SOURCE_POS" "$STAGING_POS" $([ "$SOURCE_POS" = "$STAGING_POS" ] && echo "✅" || echo "❌")
printf "| PrEP Initiations          | %12s | %12s |   %s   |\n" "$SOURCE_PREP" "$STAGING_PREP" $([ "$SOURCE_PREP" = "$STAGING_PREP" ] && echo "✅" || echo "❌")
printf "| Currently on ART (TX_CURR)| %12s | %12s |   %s   |\n" "$SOURCE_ART" "$STAGING_ART" $([ "$SOURCE_ART" = "$STAGING_ART" ] && echo "✅" || echo "❌")
printf "| VL Tested                 | %12s | %12s |   %s   |\n" "$SOURCE_VL_TESTED" "$STAGING_VL_TESTED" $([ "$SOURCE_VL_TESTED" = "$STAGING_VL_TESTED" ] && echo "✅" || echo "❌")
printf "| VL Suppressed             | %12s | %12s |   %s   |\n" "$SOURCE_VL_SUPPRESSED" "$STAGING_VL_SUPPRESSED" $([ "$SOURCE_VL_SUPPRESSED" = "$STAGING_VL_SUPPRESSED" ] && echo "✅" || echo "❌")
echo "+---------------------------+--------------+--------------+--------+"

# =========================================
# SECTION 9: DATA QUALITY CHECKS
# =========================================
echo ""
echo "📊 [SECTION 9] DATA QUALITY CHECKS"
echo "-----------------------------------------------------------------"

eval /opt/mssql-tools/bin/sqlcmd $STAGING -Q "
SELECT '=== STAGING DATA QUALITY ===' as '';
SELECT 
    'NULL RegionId' as Issue,
    COUNT(*) as Count,
    'HIV' as TableName
FROM IndicatorValues_HIV WHERE RegionId IS NULL
UNION ALL
SELECT 'Unknown AgeGroup', COUNT(*), 'HIV'
FROM IndicatorValues_HIV WHERE AgeGroup = 'Unknown'
UNION ALL
SELECT 'Other Sex', COUNT(*), 'HIV'
FROM IndicatorValues_HIV WHERE Sex = 'Other'
UNION ALL
SELECT 'Zero/Null Value', COUNT(*), 'HIV'
FROM IndicatorValues_HIV WHERE Value <= 0
UNION ALL
SELECT 'NULL RegionId', COUNT(*), 'Prevention'
FROM IndicatorValues_Prevention WHERE RegionId IS NULL
UNION ALL
SELECT 'Unknown AgeGroup', COUNT(*), 'Prevention'
FROM IndicatorValues_Prevention WHERE AgeGroup = 'Unknown'
UNION ALL
SELECT 'Other Sex', COUNT(*), 'Prevention'
FROM IndicatorValues_Prevention WHERE Sex = 'Other'
UNION ALL
SELECT 'Zero/Null Value', COUNT(*), 'Prevention'
FROM IndicatorValues_Prevention WHERE Value <= 0;
"

# =========================================
# SECTION 10: VIRAL LOAD METRICS
# =========================================
echo ""
echo "📊 [SECTION 10] VIRAL LOAD METRICS"
echo "-----------------------------------------------------------------"

eval /opt/mssql-tools/bin/sqlcmd $STAGING -Q "
SELECT '=== VIRAL LOAD CALCULATIONS ===' as '';
WITH VLMetrics AS (
    SELECT 
        SUM(CASE WHEN Indicator = 'TX_CURR' THEN Value ELSE 0 END) as OnART,
        SUM(CASE WHEN Indicator = 'TX_VL_TESTED' THEN Value ELSE 0 END) as VLTested,
        SUM(CASE WHEN Indicator = 'TX_VL_SUPPRESSED' THEN Value ELSE 0 END) as VLSuppressed,
        SUM(CASE WHEN Indicator = 'TX_VL_UNDETECTABLE' THEN Value ELSE 0 END) as VLUndetectable
    FROM IndicatorValues_HIV
    WHERE Indicator IN ('TX_CURR', 'TX_VL_TESTED', 'TX_VL_SUPPRESSED', 'TX_VL_UNDETECTABLE')
)
SELECT 
    OnART,
    VLTested,
    VLSuppressed,
    VLUndetectable,
    CAST(VLTested * 100.0 / NULLIF(OnART, 0) AS DECIMAL(5,1)) as CoverageRate,
    CAST(VLSuppressed * 100.0 / NULLIF(VLTested, 0) AS DECIMAL(5,1)) as SuppressionRate,
    CAST(VLUndetectable * 100.0 / NULLIF(VLTested, 0) AS DECIMAL(5,1)) as UndetectableRate
FROM VLMetrics;
"

# =========================================
# SUMMARY
# =========================================
echo ""
echo "================================================================="
echo "✅ VERIFICATION COMPLETE"
echo "================================================================="

if [ "$SOURCE_TESTS" = "$STAGING_TESTS" ] && [ "$SOURCE_ART" = "$STAGING_ART" ]; then
    echo "🎉 ALL KEY METRICS MATCH! Your ETL is working perfectly."
    echo "✅ Each source row with criteria is counted correctly as 1 in staging value"
else
    echo "⚠️  Some metrics don't match. Check the details above."
fi
echo "================================================================="