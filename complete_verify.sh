#!/bin/bash

# ============================================
# COMPLETE DATA VERIFICATION SCRIPT
# Tests ALL data from source and staging
# ============================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Connection strings - Store as arrays to preserve quotes
SOURCE_ARGS=(-S "10.216.0.10,1480" -U sa -P "Support25" -d All_Dataset -C)
STAGING_ARGS=(-S "102.37.18.0,1433" -U sa -P "1!2@3#Abcd123!" -d EswatiniHealth_Staging -C)

# Helper function to run sqlcmd with source connection
run_source() {
    /opt/mssql-tools/bin/sqlcmd "${SOURCE_ARGS[@]}" "$@"
}

# Helper function to run sqlcmd with staging connection
run_staging() {
    /opt/mssql-tools/bin/sqlcmd "${STAGING_ARGS[@]}" "$@"
}

echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}🔍 COMPLETE DATA VERIFICATION REPORT${NC}"
echo -e "${BLUE}Generated: $(date)${NC}"
echo -e "${BLUE}=================================================================${NC}"
echo ""

# Test connections first
echo -e "${YELLOW}Testing Connections...${NC}"

# Test source connection using array
run_source -Q "SELECT '✅ Source Connection OK' as Status;" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Source Connection Successful${NC}"
else
    echo -e "${RED}❌ Source Connection Failed${NC}"
    echo -e "${YELLOW}Testing with direct command...${NC}"
    /opt/mssql-tools/bin/sqlcmd -S 10.216.0.10,1480 -U sa -P 'Support25' -d All_Dataset -C -Q "SELECT DB_NAME() as DatabaseName, @@VERSION as Version;"
    echo ""
    echo -e "${YELLOW}Debug: Connection arguments being used:${NC}"
    printf "%s " "${SOURCE_ARGS[@]}"
    echo ""
    exit 1
fi

# Test staging connection using array
run_staging -Q "SELECT '✅ Staging Connection OK' as Status;" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Staging Connection Successful${NC}"
else
    echo -e "${RED}❌ Staging Connection Failed${NC}"
    echo -e "${YELLOW}Testing staging connection...${NC}"
    /opt/mssql-tools/bin/sqlcmd -S 102.37.18.0,1433 -U sa -P '1!2@3#Abcd123!' -d EswatiniHealth_Staging -C -Q "SELECT DB_NAME() as DatabaseName, @@VERSION as Version;"
    echo ""
    echo -e "${YELLOW}Debug: Connection arguments being used:${NC}"
    printf "%s " "${STAGING_ARGS[@]}"
    echo ""
    exit 1
fi
echo ""

# ============================================
# SECTION 1: SOURCE DATABASE OVERVIEW
# ============================================
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 SECTION 1: SOURCE DATABASE OVERVIEW (All_Dataset)${NC}"
echo -e "${BLUE}=================================================================${NC}"

echo -e "${YELLOW}Table Row Counts:${NC}"
run_source -Q "
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
FROM tmpARTTXOutcomes
ORDER BY TableName;
" -W -w 4096

# ============================================
# SECTION 2: HTS SOURCE DETAILS
# ============================================
echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 SECTION 2: HTS SOURCE DETAILS${NC}"
echo -e "${BLUE}=================================================================${NC}"

echo -e "${YELLOW}HTS Overall Metrics:${NC}"
run_source -Q "
SELECT 
    'Total Records' as Metric, 
    COUNT(*) as Value 
FROM tmpHTSTestedDetail
UNION ALL SELECT 'Tested for HIV', SUM(HTS_TestedForHIV) FROM tmpHTSTestedDetail
UNION ALL SELECT 'Tested Negative', SUM(HTS_TestedNegative) FROM tmpHTSTestedDetail
UNION ALL SELECT 'Tested Positive', SUM(HTS_TestedPositive) FROM tmpHTSTestedDetail
UNION ALL SELECT 'Linked to ART', SUM(HTS_TestedPositiveInitiatedOnART) FROM tmpHTSTestedDetail
UNION ALL SELECT 'Already on ART', SUM(HTS_TestedPosAlreadyOnART) FROM tmpHTSTestedDetail
UNION ALL SELECT 'Screened for TB', SUM(HTS_ScreenedForTB) FROM tmpHTSTestedDetail;
" -W -w 4096

echo -e "${YELLOW}HTS by Region:${NC}"
run_source -Q "
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
" -W -w 4096

echo -e "${YELLOW}HTS by Age Group:${NC}"
run_source -Q "
SELECT 
    AgeGroup,
    COUNT(*) as TotalVisits,
    SUM(HTS_TestedForHIV) as Tested,
    SUM(HTS_TestedPositive) as Positive,
    SUM(HTS_TestedNegative) as Negative
FROM tmpHTSTestedDetail
WHERE AgeGroup IS NOT NULL
GROUP BY AgeGroup
ORDER BY AgeGroup;
" -W -w 4096

echo -e "${YELLOW}HTS by Sex:${NC}"
run_source -Q "
SELECT 
    CASE Sex WHEN 1 THEN 'Male' WHEN 2 THEN 'Female' ELSE 'Other' END as Sex,
    COUNT(*) as TotalVisits,
    SUM(HTS_TestedForHIV) as Tested,
    SUM(HTS_TestedPositive) as Positive
FROM tmpHTSTestedDetail
GROUP BY Sex
ORDER BY Sex;
" -W -w 4096

# ============================================
# SECTION 3: PrEP SOURCE DETAILS
# ============================================
echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 SECTION 3: PrEP SOURCE DETAILS${NC}"
echo -e "${BLUE}=================================================================${NC}"

echo -e "${YELLOW}LineListingsPrep Metrics:${NC}"
run_source -Q "
SELECT 
    'Total Records' as Metric,
    COUNT(*) as Value
FROM LineListingsPrep
UNION ALL SELECT 'PrEP Initiations', SUM(PrEP_Initiation) FROM LineListingsPrep
UNION ALL SELECT 'Tested for HIV', SUM(PrEP_TestedForHIV) FROM LineListingsPrep
UNION ALL SELECT 'Tested Negative', SUM(PrEP_TestedNegative) FROM LineListingsPrep
UNION ALL SELECT 'Tested Positive (Seroconverted)', SUM(PrEP_TestedPositive) FROM LineListingsPrep
UNION ALL SELECT 'Initiated on ART', SUM(PrEP_InitiatedOnART) FROM LineListingsPrep
UNION ALL SELECT 'Oral PrEP', SUM(CASE WHEN CurrentPrepMethod LIKE '%Oral%' THEN 1 ELSE 0 END) FROM LineListingsPrep
UNION ALL SELECT 'CAB-LA', SUM(CASE WHEN CurrentPrepMethod LIKE '%CAB-LA%' THEN 1 ELSE 0 END) FROM LineListingsPrep
UNION ALL SELECT 'DPV-VR', SUM(CASE WHEN CurrentPrepMethod LIKE '%DPV-VR%' THEN 1 ELSE 0 END) FROM LineListingsPrep;
" -W -w 4096

echo -e "${YELLOW}aPrepDetail Metrics:${NC}"
run_source -Q "
SELECT 
    'Total Records' as Metric,
    COUNT(*) as Value
FROM aPrepDetail
UNION ALL SELECT 'Seroconverted', 
    SUM(CASE WHEN Seroconverted IN ('1', 'true', 'yes') THEN 1 ELSE 0 END) FROM aPrepDetail
UNION ALL SELECT 'Initiated on ART', 
    SUM(CASE WHEN InitiatedOnART IN ('1', 'true', 'yes') THEN 1 ELSE 0 END) FROM aPrepDetail;
" -W -w 4096

echo -e "${YELLOW}PrEP by Region:${NC}"
run_source -Q "
SELECT 
    Region,
    COUNT(*) as TotalRecords,
    SUM(CASE WHEN Seroconverted IN ('1', 'true', 'yes') THEN 1 ELSE 0 END) as Seroconversions,
    SUM(CASE WHEN InitiatedOnART IN ('1', 'true', 'yes') THEN 1 ELSE 0 END) as LinkedToART
FROM aPrepDetail
WHERE Region IS NOT NULL
GROUP BY Region
ORDER BY Region;
" -W -w 4096

# ============================================
# SECTION 4: ART SOURCE DETAILS
# ============================================
echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 SECTION 4: ART SOURCE DETAILS${NC}"
echo -e "${BLUE}=================================================================${NC}"

echo -e "${YELLOW}ART Overall Metrics:${NC}"
run_source -Q "
SELECT 
    'Total Records' as Metric,
    COUNT(*) as Value
FROM tmpARTTXOutcomes
UNION ALL SELECT 'Currently on ART (TX_CURR)', SUM(TX_CURR) FROM tmpARTTXOutcomes
UNION ALL SELECT 'VL Tested', SUM(TX_VLTested) FROM tmpARTTXOutcomes
UNION ALL SELECT 'VL Suppressed', SUM(TX_VLSuppressed) FROM tmpARTTXOutcomes
UNION ALL SELECT 'VL Unsuppressed', SUM(TX_VLUnSuppressed) FROM tmpARTTXOutcomes
UNION ALL SELECT 'VL Undetectable', SUM(TX_VLUndetectable) FROM tmpARTTXOutcomes
UNION ALL SELECT 'Deceased', SUM(TX_Deceased) FROM tmpARTTXOutcomes
UNION ALL SELECT 'IIT', SUM(TX_IIT) FROM tmpARTTXOutcomes
UNION ALL SELECT 'RTT', SUM(TX_RTT) FROM tmpARTTXOutcomes
UNION ALL SELECT 'Transfer Out', SUM(TX_TransferOut) FROM tmpARTTXOutcomes
UNION ALL SELECT 'Stopped', SUM(TX_Stopped) FROM tmpARTTXOutcomes;
" -W -w 4096

echo -e "${YELLOW}ART by Region:${NC}"
run_source -Q "
SELECT 
    ISNULL(r.Region, 'Unknown') as Region,
    COUNT(*) as RecordCount,
    SUM(a.TX_CURR) as OnART,
    SUM(a.TX_VLTested) as VLTested,
    SUM(a.TX_VLSuppressed) as VLSuppressed,
    SUM(a.TX_VLUndetectable) as VLUndetectable
FROM tmpARTTXOutcomes a
LEFT JOIN (SELECT DISTINCT FacilityCode, Region FROM aPrepDetail WHERE FacilityCode IS NOT NULL) r 
    ON a.FacilityCode = r.FacilityCode
GROUP BY r.Region
ORDER BY r.Region;
" -W -w 4096

echo -e "${YELLOW}ART by Age Group:${NC}"
run_source -Q "
SELECT 
    AgeGroup,
    COUNT(*) as RecordCount,
    SUM(TX_CURR) as OnART,
    SUM(TX_VLTested) as VLTested,
    SUM(TX_VLSuppressed) as VLSuppressed
FROM tmpARTTXOutcomes
WHERE AgeGroup IS NOT NULL
GROUP BY AgeGroup
ORDER BY AgeGroup;
" -W -w 4096

echo -e "${YELLOW}ART by Sex:${NC}"
run_source -Q "
SELECT 
    SexName,
    COUNT(*) as RecordCount,
    SUM(TX_CURR) as OnART,
    SUM(TX_VLTested) as VLTested,
    SUM(TX_VLSuppressed) as VLSuppressed
FROM tmpARTTXOutcomes
GROUP BY SexName
ORDER BY SexName;
" -W -w 4096

# ============================================
# SECTION 5: STAGING DATABASE OVERVIEW
# ============================================
echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 SECTION 5: STAGING DATABASE OVERVIEW${NC}"
echo -e "${BLUE}=================================================================${NC}"

echo -e "${YELLOW}Staging Table Summary:${NC}"
run_staging -Q "
SELECT 
    'IndicatorValues_HIV' as TableName,
    COUNT(*) as AggregatedRows,
    SUM(Value) as TotalValue,
    COUNT(DISTINCT Indicator) as UniqueIndicators,
    MIN(VisitDate) as EarliestDate,
    MAX(VisitDate) as LatestDate
FROM IndicatorValues_HIV
UNION ALL
SELECT 
    'IndicatorValues_Prevention',
    COUNT(*),
    SUM(Value),
    COUNT(DISTINCT Indicator),
    MIN(VisitDate),
    MAX(VisitDate)
FROM IndicatorValues_Prevention
UNION ALL
SELECT 
    'IndicatorValues_TB',
    COUNT(*),
    SUM(Value),
    COUNT(DISTINCT Indicator),
    MIN(VisitDate),
    MAX(VisitDate)
FROM IndicatorValues_TB
UNION ALL
SELECT 
    'Users',
    COUNT(*),
    NULL,
    NULL,
    NULL,
    NULL
FROM Users
UNION ALL
SELECT 
    'IndicatorTargets',
    COUNT(*),
    NULL,
    NULL,
    NULL,
    NULL
FROM IndicatorTargets
ORDER BY TableName;
" -W -w 4096

# ============================================
# SECTION 6: STAGING HIV DETAILS
# ============================================
echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 SECTION 6: STAGING HIV DETAILS${NC}"
echo -e "${BLUE}=================================================================${NC}"

echo -e "${YELLOW}HIV Indicators Summary:${NC}"
run_staging -Q "
SELECT 
    Indicator,
    COUNT(*) as AggregatedRows,
    SUM(Value) as TotalPatients,
    MIN(VisitDate) as EarliestDate,
    MAX(VisitDate) as LatestDate
FROM IndicatorValues_HIV
GROUP BY Indicator
ORDER BY Indicator;
" -W -w 4096

echo -e "${YELLOW}HIV by Region:${NC}"
run_staging -Q "
SELECT 
    CASE RegionId
        WHEN 1 THEN 'Hhohho'
        WHEN 2 THEN 'Manzini'
        WHEN 3 THEN 'Shiselweni'
        WHEN 4 THEN 'Lubombo'
    END as Region,
    SUM(CASE WHEN Indicator = 'TX_CURR' THEN Value ELSE 0 END) as OnART,
    SUM(CASE WHEN Indicator = 'TX_VL_TESTED' THEN Value ELSE 0 END) as VLTested,
    SUM(CASE WHEN Indicator = 'TX_VL_SUPPRESSED' THEN Value ELSE 0 END) as VLSuppressed,
    SUM(CASE WHEN Indicator = 'TX_VL_UNDETECTABLE' THEN Value ELSE 0 END) as VLUndetectable
FROM IndicatorValues_HIV
GROUP BY RegionId
ORDER BY RegionId;
" -W -w 4096

echo -e "${YELLOW}HIV by Age Group:${NC}"
run_staging -Q "
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
" -W -w 4096

echo -e "${YELLOW}HIV by Sex:${NC}"
run_staging -Q "
SELECT 
    Sex,
    SUM(Value) as TotalOnART,
    CAST(SUM(Value) * 100.0 / (SELECT SUM(Value) FROM IndicatorValues_HIV WHERE Indicator = 'TX_CURR') AS DECIMAL(5,1)) as Percentage
FROM IndicatorValues_HIV
WHERE Indicator = 'TX_CURR'
GROUP BY Sex
ORDER BY Sex;
" -W -w 4096

# ============================================
# SECTION 7: STAGING PREVENTION DETAILS
# ============================================
echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 SECTION 7: STAGING PREVENTION DETAILS${NC}"
echo -e "${BLUE}=================================================================${NC}"

echo -e "${YELLOW}Prevention Indicators Summary:${NC}"
run_staging -Q "
SELECT 
    Indicator,
    COUNT(*) as AggregatedRows,
    SUM(Value) as TotalCount,
    MIN(VisitDate) as EarliestDate,
    MAX(VisitDate) as LatestDate
FROM IndicatorValues_Prevention
GROUP BY Indicator
ORDER BY Indicator;
" -W -w 4096

echo -e "${YELLOW}Prevention by Region:${NC}"
run_staging -Q "
SELECT 
    CASE p.RegionId
        WHEN 1 THEN 'Hhohho'
        WHEN 2 THEN 'Manzini'
        WHEN 3 THEN 'Shiselweni'
        WHEN 4 THEN 'Lubombo'
    END as Region,
    SUM(CASE WHEN Indicator = 'HTS_TST' THEN Value ELSE 0 END) as Tests,
    SUM(CASE WHEN Indicator = 'HTS_POS' THEN Value ELSE 0 END) as Positives,
    SUM(CASE WHEN Indicator = 'HTS_NEG' THEN Value ELSE 0 END) as Negatives,
    SUM(CASE WHEN Indicator = 'PREP_NEW' THEN Value ELSE 0 END) as PrEPInitiations,
    SUM(CASE WHEN Indicator = 'PREP_SEROCONVERSION' THEN Value ELSE 0 END) as Seroconversions,
    SUM(CASE WHEN Indicator = 'PREP_LINKAGE_ART' THEN Value ELSE 0 END) as LinkedToART
FROM IndicatorValues_Prevention p
GROUP BY p.RegionId
ORDER BY p.RegionId;
" -W -w 4096

echo -e "${YELLOW}HTS by Age Group:${NC}"
run_staging -Q "
SELECT 
    AgeGroup,
    SUM(CASE WHEN Indicator = 'HTS_TST' THEN Value ELSE 0 END) as Tests,
    SUM(CASE WHEN Indicator = 'HTS_POS' THEN Value ELSE 0 END) as Positives,
    SUM(CASE WHEN Indicator = 'HTS_NEG' THEN Value ELSE 0 END) as Negatives,
    CAST(SUM(CASE WHEN Indicator = 'HTS_POS' THEN Value ELSE 0 END) * 100.0 / 
         NULLIF(SUM(CASE WHEN Indicator = 'HTS_TST' THEN Value ELSE 0 END), 0) AS DECIMAL(5,1)) as PositivityRate
FROM IndicatorValues_Prevention
WHERE Indicator IN ('HTS_TST', 'HTS_POS', 'HTS_NEG')
GROUP BY AgeGroup
ORDER BY AgeGroup;
" -W -w 4096

# ============================================
# SECTION 8: DIRECT COMPARISON (SOURCE vs STAGING)
# ============================================
echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 SECTION 8: DIRECT COMPARISON (SOURCE vs STAGING)${NC}"
echo -e "${BLUE}=================================================================${NC}"

# Get all source totals
echo -e "${YELLOW}Fetching source totals...${NC}"
SOURCE_TESTS=$(run_source -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(HTS_TestedForHIV), 0) FROM tmpHTSTestedDetail;" | tr -d ' ' | tr -d '\r')
SOURCE_POS=$(run_source -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(HTS_TestedPositive), 0) FROM tmpHTSTestedDetail;" | tr -d ' ' | tr -d '\r')
SOURCE_NEG=$(run_source -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(HTS_TestedNegative), 0) FROM tmpHTSTestedDetail;" | tr -d ' ' | tr -d '\r')
SOURCE_LINK=$(run_source -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(HTS_TestedPositiveInitiatedOnART), 0) FROM tmpHTSTestedDetail;" | tr -d ' ' | tr -d '\r')
SOURCE_PREP=$(run_source -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(PrEP_Initiation), 0) FROM LineListingsPrep;" | tr -d ' ' | tr -d '\r')
SOURCE_SERO=$(run_source -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(CASE WHEN Seroconverted IN ('1', 'true', 'yes') THEN 1 ELSE 0 END), 0) FROM aPrepDetail;" | tr -d ' ' | tr -d '\r')
SOURCE_ART=$(run_source -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(TX_CURR), 0) FROM tmpARTTXOutcomes;" | tr -d ' ' | tr -d '\r')
SOURCE_VL_TESTED=$(run_source -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(TX_VLTested), 0) FROM tmpARTTXOutcomes;" | tr -d ' ' | tr -d '\r')
SOURCE_VL_SUPPRESSED=$(run_source -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(TX_VLSuppressed), 0) FROM tmpARTTXOutcomes;" | tr -d ' ' | tr -d '\r')
SOURCE_VL_UNDETECTABLE=$(run_source -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(TX_VLUndetectable), 0) FROM tmpARTTXOutcomes;" | tr -d ' ' | tr -d '\r')

# Get all staging totals
echo -e "${YELLOW}Fetching staging totals...${NC}"
STAGING_TESTS=$(run_staging -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_Prevention WHERE Indicator='HTS_TST';" | tr -d ' ' | tr -d '\r')
STAGING_POS=$(run_staging -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_Prevention WHERE Indicator='HTS_POS';" | tr -d ' ' | tr -d '\r')
STAGING_NEG=$(run_staging -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_Prevention WHERE Indicator='HTS_NEG';" | tr -d ' ' | tr -d '\r')
STAGING_LINK=$(run_staging -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_Prevention WHERE Indicator='LINKAGE_ART';" | tr -d ' ' | tr -d '\r')
STAGING_PREP=$(run_staging -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_Prevention WHERE Indicator='PREP_NEW';" | tr -d ' ' | tr -d '\r')
STAGING_SERO=$(run_staging -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_Prevention WHERE Indicator='PREP_SEROCONVERSION';" | tr -d ' ' | tr -d '\r')
STAGING_ART=$(run_staging -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_HIV WHERE Indicator='TX_CURR';" | tr -d ' ' | tr -d '\r')
STAGING_VL_TESTED=$(run_staging -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_HIV WHERE Indicator='TX_VL_TESTED';" | tr -d ' ' | tr -d '\r')
STAGING_VL_SUPPRESSED=$(run_staging -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_HIV WHERE Indicator='TX_VL_SUPPRESSED';" | tr -d ' ' | tr -d '\r')
STAGING_VL_UNDETECTABLE=$(run_staging -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(SUM(Value), 0) FROM IndicatorValues_HIV WHERE Indicator='TX_VL_UNDETECTABLE';" | tr -d ' ' | tr -d '\r')

# Display comparison table
echo ""
echo -e "${YELLOW}COMPARISON TABLE (Source vs Staging):${NC}"
echo "+------------------------------------+--------------+--------------+--------+"
echo "| Metric                             | Source       | Staging      | Match? |"
echo "+------------------------------------+--------------+--------------+--------+"

printf "| HTS Tests                          | %12s | %12s |   %s   |\n" "$SOURCE_TESTS" "$STAGING_TESTS" $([ "$SOURCE_TESTS" = "$STAGING_TESTS" ] && echo "✅" || echo "❌")
printf "| HTS Positives                      | %12s | %12s |   %s   |\n" "$SOURCE_POS" "$STAGING_POS" $([ "$SOURCE_POS" = "$STAGING_POS" ] && echo "✅" || echo "❌")
printf "| HTS Negatives                      | %12s | %12s |   %s   |\n" "$SOURCE_NEG" "$STAGING_NEG" $([ "$SOURCE_NEG" = "$STAGING_NEG" ] && echo "✅" || echo "❌")
printf "| HTS Linkage                        | %12s | %12s |   %s   |\n" "$SOURCE_LINK" "$STAGING_LINK" $([ "$SOURCE_LINK" = "$STAGING_LINK" ] && echo "✅" || echo "❌")
printf "| PrEP Initiations                   | %12s | %12s |   %s   |\n" "$SOURCE_PREP" "$STAGING_PREP" $([ "$SOURCE_PREP" = "$STAGING_PREP" ] && echo "✅" || echo "❌")
printf "| PrEP Seroconversions               | %12s | %12s |   %s   |\n" "$SOURCE_SERO" "$STAGING_SERO" $([ "$SOURCE_SERO" = "$STAGING_SERO" ] && echo "✅" || echo "❌")
printf "| Currently on ART (TX_CURR)         | %12s | %12s |   %s   |\n" "$SOURCE_ART" "$STAGING_ART" $([ "$SOURCE_ART" = "$STAGING_ART" ] && echo "✅" || echo "❌")
printf "| VL Tested                          | %12s | %12s |   %s   |\n" "$SOURCE_VL_TESTED" "$STAGING_VL_TESTED" $([ "$SOURCE_VL_TESTED" = "$STAGING_VL_TESTED" ] && echo "✅" || echo "❌")
printf "| VL Suppressed                      | %12s | %12s |   %s   |\n" "$SOURCE_VL_SUPPRESSED" "$STAGING_VL_SUPPRESSED" $([ "$SOURCE_VL_SUPPRESSED" = "$STAGING_VL_SUPPRESSED" ] && echo "✅" || echo "❌")
printf "| VL Undetectable                    | %12s | %12s |   %s   |\n" "$SOURCE_VL_UNDETECTABLE" "$STAGING_VL_UNDETECTABLE" $([ "$SOURCE_VL_UNDETECTABLE" = "$STAGING_VL_UNDETECTABLE" ] && echo "✅" || echo "❌")
echo "+------------------------------------+--------------+--------------+--------+"

# ============================================
# SECTION 9: DATA QUALITY CHECKS
# ============================================
echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 SECTION 9: DATA QUALITY CHECKS${NC}"
echo -e "${BLUE}=================================================================${NC}"

echo -e "${YELLOW}Staging Data Quality Issues:${NC}"
run_staging -Q "
SELECT 
    'HIV - NULL RegionId' as Issue,
    COUNT(*) as Count
FROM IndicatorValues_HIV WHERE RegionId IS NULL
UNION ALL SELECT 'HIV - Unknown AgeGroup', COUNT(*) FROM IndicatorValues_HIV WHERE AgeGroup = 'Unknown'
UNION ALL SELECT 'HIV - Other Sex', COUNT(*) FROM IndicatorValues_HIV WHERE Sex = 'Other'
UNION ALL SELECT 'HIV - Zero/Null Value', COUNT(*) FROM IndicatorValues_HIV WHERE Value <= 0
UNION ALL SELECT 'Prevention - NULL RegionId', COUNT(*) FROM IndicatorValues_Prevention WHERE RegionId IS NULL
UNION ALL SELECT 'Prevention - Unknown AgeGroup', COUNT(*) FROM IndicatorValues_Prevention WHERE AgeGroup = 'Unknown'
UNION ALL SELECT 'Prevention - Other Sex', COUNT(*) FROM IndicatorValues_Prevention WHERE Sex = 'Other'
UNION ALL SELECT 'Prevention - Zero/Null Value', COUNT(*) FROM IndicatorValues_Prevention WHERE Value <= 0;
" -W -w 4096

# ============================================
# SECTION 10: VIRAL LOAD METRICS
# ============================================
echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 SECTION 10: VIRAL LOAD METRICS${NC}"
echo -e "${BLUE}=================================================================${NC}"

echo -e "${YELLOW}Viral Load Calculations (Staging):${NC}"
run_staging -Q "
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
    OnART as 'Total on ART',
    VLTested as 'VL Tested',
    VLSuppressed as 'VL Suppressed',
    VLUndetectable as 'VL Undetectable',
    CAST(VLTested * 100.0 / NULLIF(OnART, 0) AS DECIMAL(5,1)) as 'Coverage Rate %',
    CAST(VLSuppressed * 100.0 / NULLIF(VLTested, 0) AS DECIMAL(5,1)) as 'Suppression Rate %',
    CAST(VLUndetectable * 100.0 / NULLIF(VLTested, 0) AS DECIMAL(5,1)) as 'Undetectable Rate %'
FROM VLMetrics;
" -W -w 4096

# ============================================
# FINAL SUMMARY
# ============================================
echo ""
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}✅ VERIFICATION COMPLETE${NC}"
echo -e "${BLUE}=================================================================${NC}"

# Count how many metrics match
MATCH_COUNT=0
TOTAL_METRICS=10

[ "$SOURCE_TESTS" = "$STAGING_TESTS" ] && MATCH_COUNT=$((MATCH_COUNT+1))
[ "$SOURCE_POS" = "$STAGING_POS" ] && MATCH_COUNT=$((MATCH_COUNT+1))
[ "$SOURCE_NEG" = "$STAGING_NEG" ] && MATCH_COUNT=$((MATCH_COUNT+1))
[ "$SOURCE_LINK" = "$STAGING_LINK" ] && MATCH_COUNT=$((MATCH_COUNT+1))
[ "$SOURCE_PREP" = "$STAGING_PREP" ] && MATCH_COUNT=$((MATCH_COUNT+1))
[ "$SOURCE_SERO" = "$STAGING_SERO" ] && MATCH_COUNT=$((MATCH_COUNT+1))
[ "$SOURCE_ART" = "$STAGING_ART" ] && MATCH_COUNT=$((MATCH_COUNT+1))
[ "$SOURCE_VL_TESTED" = "$STAGING_VL_TESTED" ] && MATCH_COUNT=$((MATCH_COUNT+1))
[ "$SOURCE_VL_SUPPRESSED" = "$STAGING_VL_SUPPRESSED" ] && MATCH_COUNT=$((MATCH_COUNT+1))
[ "$SOURCE_VL_UNDETECTABLE" = "$STAGING_VL_UNDETECTABLE" ] && MATCH_COUNT=$((MATCH_COUNT+1))

echo -e "${YELLOW}Summary: ${MATCH_COUNT}/${TOTAL_METRICS} metrics match${NC}"

if [ $MATCH_COUNT -eq $TOTAL_METRICS ]; then
    echo -e "${GREEN}🎉 ALL DATA VERIFIED! Your ETL is working perfectly.${NC}"
    echo -e "${GREEN}✅ Each source row is correctly aggregated in staging.${NC}"
else
    echo -e "${RED}⚠️  Some metrics don't match. Check the comparison table above.${NC}"
fi

echo -e "${BLUE}=================================================================${NC}"