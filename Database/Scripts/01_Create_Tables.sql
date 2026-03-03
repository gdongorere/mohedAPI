-- =====================================================
-- PART 1: CREATE ALL TABLES
-- =====================================================

SET QUOTED_IDENTIFIER ON;
GO

-- Drop tables if they exist (for clean setup)
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'IndicatorValues_TB') DROP TABLE IndicatorValues_TB;
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'IndicatorValues_Prevention') DROP TABLE IndicatorValues_Prevention;
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'IndicatorValues_HIV') DROP TABLE IndicatorValues_HIV;
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'Users') DROP TABLE Users;
GO

-- =====================================================
-- 1. USERS TABLE (for authentication)
-- =====================================================
CREATE TABLE Users (
    Id NVARCHAR(36) PRIMARY KEY,
    Email NVARCHAR(255) NOT NULL,
    Name NVARCHAR(255) NOT NULL,
    Surname NVARCHAR(255) NULL,
    Role NVARCHAR(50) NOT NULL DEFAULT 'viewer',
    PasswordHash NVARCHAR(500) NOT NULL,
    LastLoginAt DATETIME2 NULL,
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    UpdatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE UNIQUE INDEX IX_Users_Email ON Users(Email);
GO

-- =====================================================
-- 2. HIV/TREATMENT INDICATOR VALUES
-- =====================================================
CREATE TABLE IndicatorValues_HIV (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    Indicator NVARCHAR(50) NOT NULL,
    RegionId INT NOT NULL,
    VisitDate DATE NOT NULL,
    AgeGroup NVARCHAR(20) NOT NULL,
    Sex NVARCHAR(10) NOT NULL,
    PopulationType NVARCHAR(50) NULL,
    Value INT NOT NULL,
    CreatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    UpdatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    
    CONSTRAINT CHK_HIV_RegionId CHECK (RegionId BETWEEN 1 AND 4),
    CONSTRAINT CHK_HIV_Sex CHECK (Sex IN ('M', 'F', 'Other'))
);
GO

CREATE INDEX IX_HIV_Lookup ON IndicatorValues_HIV (Indicator, RegionId, VisitDate, AgeGroup, Sex, PopulationType);
CREATE INDEX IX_HIV_DateRange ON IndicatorValues_HIV (VisitDate);
CREATE INDEX IX_HIV_Updated ON IndicatorValues_HIV (UpdatedAt);
GO

-- =====================================================
-- 3. PREVENTION INDICATOR VALUES
-- =====================================================
CREATE TABLE IndicatorValues_Prevention (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    Indicator NVARCHAR(50) NOT NULL,
    RegionId INT NOT NULL,
    VisitDate DATE NOT NULL,
    AgeGroup NVARCHAR(20) NOT NULL,
    Sex NVARCHAR(10) NOT NULL,
    PopulationType NVARCHAR(50) NULL,
    Value INT NOT NULL,
    CreatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    UpdatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    
    CONSTRAINT CHK_Prevention_RegionId CHECK (RegionId BETWEEN 1 AND 4),
    CONSTRAINT CHK_Prevention_Sex CHECK (Sex IN ('M', 'F', 'Other'))
);
GO

CREATE INDEX IX_Prevention_Lookup ON IndicatorValues_Prevention (Indicator, RegionId, VisitDate, AgeGroup, Sex, PopulationType);
CREATE INDEX IX_Prevention_DateRange ON IndicatorValues_Prevention (VisitDate);
CREATE INDEX IX_Prevention_Updated ON IndicatorValues_Prevention (UpdatedAt);
GO

-- =====================================================
-- 4. TB INDICATOR VALUES
-- =====================================================
CREATE TABLE IndicatorValues_TB (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    Indicator NVARCHAR(50) NOT NULL,
    RegionId INT NOT NULL,
    VisitDate DATE NOT NULL,
    AgeGroup NVARCHAR(20) NOT NULL,
    Sex NVARCHAR(10) NOT NULL,
    TBType NVARCHAR(50) NULL,
    PopulationType NVARCHAR(50) NULL,
    Value INT NOT NULL,
    CreatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    UpdatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    
    CONSTRAINT CHK_TB_RegionId CHECK (RegionId BETWEEN 1 AND 4),
    CONSTRAINT CHK_TB_Sex CHECK (Sex IN ('M', 'F', 'Other'))
);
GO

CREATE INDEX IX_TB_Lookup ON IndicatorValues_TB (Indicator, RegionId, VisitDate, AgeGroup, Sex, TBType, PopulationType);
CREATE INDEX IX_TB_DateRange ON IndicatorValues_TB (VisitDate);
CREATE INDEX IX_TB_Updated ON IndicatorValues_TB (UpdatedAt);
GO

PRINT 'All tables created successfully!';
GO

-- =====================================================
-- 5: CREATE TARGETS TABLE
-- =====================================================

SET QUOTED_IDENTIFIER ON;
GO

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'IndicatorTargets') DROP TABLE IndicatorTargets;
GO

CREATE TABLE IndicatorTargets (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Indicator NVARCHAR(50) NOT NULL,
    RegionId INT NULL,
    Year INT NOT NULL,
    Quarter INT NULL,
    Month INT NULL,
    TargetValue DECIMAL(18,2) NOT NULL,
    TargetType NVARCHAR(20) NOT NULL DEFAULT 'number',
    Notes NVARCHAR(500) NULL,
    AgeGroup NVARCHAR(20) NULL,
    Sex NVARCHAR(10) NULL,
    PopulationType NVARCHAR(50) NULL,
    CreatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    UpdatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CreatedBy NVARCHAR(36) NOT NULL,
    
    CONSTRAINT CHK_Target_Quarter CHECK (Quarter BETWEEN 1 AND 4),
    CONSTRAINT CHK_Target_Month CHECK (Month BETWEEN 1 AND 12),
    CONSTRAINT CHK_Target_RegionId CHECK (RegionId BETWEEN 1 AND 4),
    CONSTRAINT CHK_Target_Sex CHECK (Sex IN ('M', 'F', 'Other', NULL)),
    CONSTRAINT CHK_Target_Type CHECK (TargetType IN ('number', 'percentage', 'rate'))
);

-- Indexes for fast lookups
CREATE INDEX IX_Targets_Lookup ON IndicatorTargets (Indicator, RegionId, Year, Quarter, Month);
CREATE INDEX IX_Targets_Indicator ON IndicatorTargets (Indicator);
CREATE INDEX IX_Targets_Region ON IndicatorTargets (RegionId);
CREATE INDEX IX_Targets_Period ON IndicatorTargets (Year, Quarter, Month);
GO

PRINT 'IndicatorTargets table created successfully!';
GO