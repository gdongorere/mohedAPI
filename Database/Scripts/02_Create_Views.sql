-- =====================================================
-- PART 2: CREATE VIEWS (Using UpdatedAt for latest records)
-- =====================================================

SET QUOTED_IDENTIFIER ON;
GO

-- =====================================================
-- 1. LATEST HIV VALUES VIEW
-- =====================================================
CREATE VIEW vw_LatestIndicatorValues_HIV AS
WITH RankedHIV AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY Indicator, RegionId, CAST(VisitDate AS DATE), 
                            AgeGroup, Sex, ISNULL(PopulationType, '')
               ORDER BY UpdatedAt DESC, Id DESC
           ) AS rn
    FROM IndicatorValues_HIV
)
SELECT Id, Indicator, RegionId, VisitDate, AgeGroup, Sex, PopulationType, 
       Value, CreatedAt, UpdatedAt
FROM RankedHIV
WHERE rn = 1;
GO

-- =====================================================
-- 2. LATEST PREVENTION VALUES VIEW
-- =====================================================
CREATE VIEW vw_LatestIndicatorValues_Prevention AS
WITH RankedPrevention AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY Indicator, RegionId, CAST(VisitDate AS DATE), 
                            AgeGroup, Sex, ISNULL(PopulationType, '')
               ORDER BY UpdatedAt DESC, Id DESC
           ) AS rn
    FROM IndicatorValues_Prevention
)
SELECT Id, Indicator, RegionId, VisitDate, AgeGroup, Sex, PopulationType, 
       Value, CreatedAt, UpdatedAt
FROM RankedPrevention
WHERE rn = 1;
GO

-- =====================================================
-- 3. LATEST TB VALUES VIEW
-- =====================================================
CREATE VIEW vw_LatestIndicatorValues_TB AS
WITH RankedTB AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY Indicator, RegionId, CAST(VisitDate AS DATE), 
                            AgeGroup, Sex, ISNULL(TBType, ''), ISNULL(PopulationType, '')
               ORDER BY UpdatedAt DESC, Id DESC
           ) AS rn
    FROM IndicatorValues_TB
)
SELECT Id, Indicator, RegionId, VisitDate, AgeGroup, Sex, TBType, PopulationType, 
       Value, CreatedAt, UpdatedAt
FROM RankedTB
WHERE rn = 1;
GO

-- =====================================================
-- 4. DAILY AGGREGATION VIEW (FIXED - added Value alias)
-- =====================================================
CREATE VIEW vw_DailyIndicatorSummary AS
SELECT 
    Indicator,
    RegionId,
    VisitDate,
    AgeGroup,
    Sex,
    PopulationType,
    TBType,
    SUM(Value) AS DailyTotal,
    COUNT(*) AS RecordCount,
    MAX(UpdatedAt) AS LastUpdated
FROM (
    SELECT Indicator, RegionId, VisitDate, AgeGroup, Sex, PopulationType, 
           NULL AS TBType, Value, UpdatedAt
    FROM vw_LatestIndicatorValues_HIV
    
    UNION ALL
    
    SELECT Indicator, RegionId, VisitDate, AgeGroup, Sex, PopulationType, 
           NULL AS TBType, Value, UpdatedAt
    FROM vw_LatestIndicatorValues_Prevention
    
    UNION ALL
    
    SELECT Indicator, RegionId, VisitDate, AgeGroup, Sex, PopulationType, 
           TBType, Value, UpdatedAt
    FROM vw_LatestIndicatorValues_TB
) AS AllData
GROUP BY Indicator, RegionId, VisitDate, AgeGroup, Sex, PopulationType, TBType;
GO

-- =====================================================
-- 5. MONTHLY AGGREGATION VIEW
-- =====================================================
CREATE VIEW vw_MonthlyIndicatorSummary AS
SELECT 
    Indicator,
    RegionId,
    DATEFROMPARTS(YEAR(VisitDate), MONTH(VisitDate), 1) AS MonthStart,
    AgeGroup,
    Sex,
    PopulationType,
    TBType,
    SUM(DailyTotal) AS MonthlyTotal,
    SUM(RecordCount) AS TotalRecords,
    COUNT(DISTINCT VisitDate) AS DaysWithData,
    MAX(LastUpdated) AS LastUpdated
FROM vw_DailyIndicatorSummary
GROUP BY Indicator, RegionId, 
         DATEFROMPARTS(YEAR(VisitDate), MONTH(VisitDate), 1),
         AgeGroup, Sex, PopulationType, TBType;
GO

-- =====================================================
-- 6. QUARTERLY AGGREGATION VIEW
-- =====================================================
CREATE VIEW vw_QuarterlyIndicatorSummary AS
SELECT 
    Indicator,
    RegionId,
    DATEFROMPARTS(YEAR(MonthStart), ((MONTH(MonthStart)-1)/3)*3 + 1, 1) AS QuarterStart,
    AgeGroup,
    Sex,
    PopulationType,
    TBType,
    SUM(MonthlyTotal) AS QuarterlyTotal,
    SUM(TotalRecords) AS TotalRecords,
    SUM(DaysWithData) AS DaysWithData,
    MAX(LastUpdated) AS LastUpdated
FROM vw_MonthlyIndicatorSummary
GROUP BY Indicator, RegionId, 
         DATEFROMPARTS(YEAR(MonthStart), ((MONTH(MonthStart)-1)/3)*3 + 1, 1),
         AgeGroup, Sex, PopulationType, TBType;
GO

-- =====================================================
-- 7. REGION LOOKUP VIEW (For reference)
-- =====================================================
CREATE VIEW vw_Regions AS
SELECT 1 AS RegionId, 'Hhohho' AS RegionName
UNION ALL SELECT 2, 'Manzini'
UNION ALL SELECT 3, 'Shiselweni'
UNION ALL SELECT 4, 'Lubombo';
GO

PRINT 'All views created successfully!';
GO