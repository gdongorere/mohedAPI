-- =====================================================
-- PART 4: ADD TARGETS TABLE (Run this after tables are created)
-- =====================================================

SET QUOTED_IDENTIFIER ON;
GO

-- Check if table exists
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'IndicatorTargets')
BEGIN
    CREATE TABLE IndicatorTargets (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        Indicator NVARCHAR(50) NOT NULL,
        RegionId INT NULL,  -- NULL = National level
        [Year] INT NOT NULL,
        Quarter INT NULL,   -- 1-4
        [Month] INT NULL,   -- 1-12
        TargetValue DECIMAL(18,2) NOT NULL,
        TargetType NVARCHAR(20) NOT NULL DEFAULT 'number', -- 'number', 'percentage'
        Notes NVARCHAR(500) NULL,
        AgeGroup NVARCHAR(20) NULL,
        Sex NVARCHAR(10) NULL,
        PopulationType NVARCHAR(50) NULL,
        CreatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        UpdatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        CreatedBy NVARCHAR(36) NOT NULL,
        
        CONSTRAINT CHK_Target_RegionId CHECK (RegionId BETWEEN 1 AND 4 OR RegionId IS NULL),
        CONSTRAINT CHK_Target_Quarter CHECK (Quarter BETWEEN 1 AND 4 OR Quarter IS NULL),
        CONSTRAINT CHK_Target_Month CHECK ([Month] BETWEEN 1 AND 12 OR [Month] IS NULL),
        CONSTRAINT CHK_Target_Period CHECK (
            (Quarter IS NULL AND [Month] IS NULL) OR  -- Annual target
            (Quarter IS NOT NULL AND [Month] IS NULL) OR  -- Quarterly target
            (Quarter IS NULL AND [Month] IS NOT NULL)  -- Monthly target
        )
    );

    -- Create indexes for performance
    CREATE INDEX IX_Targets_Lookup ON IndicatorTargets (Indicator, RegionId, [Year], Quarter, [Month]);
    CREATE INDEX IX_Targets_Year ON IndicatorTargets ([Year]);
    CREATE INDEX IX_Targets_Region ON IndicatorTargets (RegionId);
    CREATE INDEX IX_Targets_Indicator ON IndicatorTargets (Indicator);
    
    PRINT 'IndicatorTargets table created successfully!';
END
ELSE
BEGIN
    PRINT 'IndicatorTargets table already exists.';
END
GO

-- Add some sample targets (optional - comment out if not needed)
---IF NOT EXISTS (SELECT 1 FROM IndicatorTargets)
--BEGIN
    -- National targets for 2025
--    INSERT INTO IndicatorTargets (Indicator, RegionId, [Year], TargetValue, TargetType, CreatedBy)
--    VALUES 
--        ('TX_CURR', NULL, 2025, 220000, 'number', 'system'),
--        ('TX_NEW', NULL, 2025, 15000, 'number', 'system'),
--        ('HTS_TST', NULL, 2025, 500000, 'number', 'system'),
--        ('HTS_POS', NULL, 2025, 25000, 'number', 'system'),
--        ('PREP_NEW', NULL, 2025, 50000, 'number', 'system');
    
    -- Regional targets for 2025
--    INSERT INTO IndicatorTargets (Indicator, RegionId, [Year], TargetValue, TargetType, CreatedBy)
--    VALUES 
--        ('TX_CURR', 1, 2025, 55000, 'number', 'system'),  -- Hhohho
--        ('TX_CURR', 2, 2025, 65000, 'number', 'system'),  -- Manzini
--        ('TX_CURR', 3, 2025, 45000, 'number', 'system'),  -- Shiselweni
--        ('TX_CURR', 4, 2025, 55000, 'number', 'system');  -- Lubombo
    
    -- Quarterly targets for Q1 2025
--    INSERT INTO IndicatorTargets (Indicator, RegionId, [Year], Quarter, TargetValue, TargetType, CreatedBy)
--    VALUES 
--        ('TX_NEW', NULL, 2025, 1, 3750, 'number', 'system'),
--        ('PREP_NEW', NULL, 2025, 1, 12500, 'number', 'system');
    
--    PRINT 'Sample targets added successfully!';
--END
--GO

-- Verify the table was created
SELECT 
    TABLE_NAME 
FROM 
    INFORMATION_SCHEMA.TABLES 
WHERE 
    TABLE_TYPE = 'BASE TABLE' 
    AND TABLE_NAME = 'IndicatorTargets';
GO

PRINT 'Targets setup completed successfully!';
GO