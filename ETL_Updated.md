# 📚 **Complete ETL System Explanation**

---

## 🏗️ **System Architecture**

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Source DB     │────▶│    ETL Service  │────▶│  Staging DB     │
│  (All_Dataset)  │     │  (Your API)     │     │ (EswatiniHealth)│
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                       │                        │
         ▼                       ▼                        ▼
  ┌──────────────┐        ┌──────────────┐         ┌──────────────┐
  │LineListings- │        │  HTSETL-     │         │Indicator-    │
  │Prep          │        │  Service     │         │Values_       │
  │aPrepDetail   │─────▶  │  PrEPETL-    │─────▶   │Prevention    │
  │tmpHTS-       │        │  Service     │         │              │
  │TestedDetail  │        │  ARTETL-     │         │Indicator-    │
  │tmpART-       │        │  Service     │         │Values_HIV    │
  │TXOutcomes    │        └──────────────┘         └──────────────┘
  └──────────────┘
```

---

## 🔄 **The ETL Process - Step by Step**

### **Step 1: Triggering the ETL**

There are **3 ways** to trigger an ETL:

#### **A. Manual Trigger (PowerShell)**

```powershell
# Trigger HTS ETL
$etlHeaders = @{
    "X-ETL-Key" = "simple-etl-key-2026"
    "Content-Type" = "application/json"
}
Invoke-RestMethod -Uri "http://localhost:5171/api/etl/trigger?source=hts" `
    -Method Post `
    -Headers $etlHeaders `
    -Body "{}"

# Trigger PrEP ETL
Invoke-RestMethod -Uri "http://localhost:5171/api/etl/trigger?source=prep" `
    -Method Post `
    -Headers $etlHeaders `
    -Body "{}"

# Trigger ART ETL
Invoke-RestMethod -Uri "http://localhost:5171/api/etl/trigger?source=art" `
    -Method Post `
    -Headers $etlHeaders `
    -Body "{}"
```

#### **B. Scheduled Trigger (automatic midnight runs)**

```csharp
// ScheduledETLService.cs runs at 00:00 every day
var sources = new[] { "hts", "prep", "art" };
foreach (var source in sources) {
    await etlService.RunETLForSourceAsync(source, "scheduler");
}
```

#### **C. Programmatic Trigger (from other parts of your code)**

```csharp
var result = await _etlService.RunETLForSourceAsync("hts", "system");
```

---

### **Step 2: Authentication & Routing**

When the ETL endpoint is called:

```csharp
// ETLEndpoints.cs
public static async Task<IResult> TriggerETL(
    [FromServices] IETLService etlService,
    [FromQuery] string source)
{
    // 1. Check API key (X-ETL-Key header)
    var triggeredBy = context.Request.Headers["X-ETL-Key"];

    // 2. Route to correct ETL service
    var result = await etlService.RunETLForSourceAsync(source, triggeredBy);

    // 3. Return result
    return Results.Ok(new { success = true, data = result });
}
```

```csharp
// ETLService.cs (the router)
public async Task<ETLResult> RunETLForSourceAsync(string source, string triggeredBy)
{
    return source.ToLower() switch
    {
        "hts" => await _htsETL.RunAsync(triggeredBy),   // HIV Testing
        "prep" => await _prepETL.RunAsync(triggeredBy), // PrEP data
        "art" => await _artETL.RunAsync(triggeredBy),   // ART treatment
        _ => throw new ArgumentException($"Unknown source: {source}")
    };
}
```

---

### **Step 3: Loading Facility Region Mappings (CRITICAL)**

Before processing any data, the ETL loads facility-to-region mappings with intelligent caching:

```csharp
// FacilityRegionService.cs - Smart caching
public async Task<Dictionary<string, int>> GetFacilityRegionsAsync()
{
    // Check cache first (valid for 1 hour)
    if (_cache.Any() && DateTime.UtcNow - _lastCacheUpdate < _cacheDuration)
    {
        return new Dictionary<string, int>(_cache);
    }

    // STEP 1: Load explicit mappings from aPrepDetail
    var explicitMappings = await LoadExplicitMappingsAsync();

    // STEP 2: Find all ART facilities that need mapping
    var artFacilities = await GetARTFacilitiesAsync();

    // STEP 3: Apply first-letter mapping to unmapped facilities
    // H → Hhohho (1), M → Manzini (2), S → Shiselweni (3), L → Lubombo (4)

    return result; // Dictionary of FacilityCode → RegionId
}
```

**What this solves**: The 876 missing ART patients! Facilities without explicit mappings get mapped by their first letter.

---

### **Step 4: SAFE Processing - Process First, Then Replace**

This is the **MOST IMPORTANT safety feature** - data is processed in memory first, then written in a transaction:

```csharp
// ARTETLService.cs - Safe processing
public async Task<ETLResult> RunAsync(string triggeredBy = "system")
{
    try
    {
        // STEP 1: Process all source data into memory FIRST
        _logger.LogInformation("Step 1: Reading and aggregating source data...");
        var (recordsRead, finalRecords) = await ProcessAndAggregateSourceDataAsync();

        // STEP 2: Only after successful processing, replace staging data
        _logger.LogInformation("Step 2: Replacing staging data...");

        // Clear existing data and insert new data
        await _db.Database.ExecuteSqlRawAsync("DELETE FROM IndicatorValues_HIV");
        await _db.IndicatorValues_HIV.AddRangeAsync(finalRecords);
        var inserted = await _db.SaveChangesAsync();

        _logger.LogInformation("Step 3: Successfully inserted {Inserted} records", inserted);
    }
    catch (Exception ex)
    {
        // If anything fails, old data remains intact!
        _logger.LogError(ex, "❌ ART ETL failed");
        throw;
    }
}
```

**Why this matters**: If the ETL fails halfway, your staging database still has the OLD data, not empty tables!

---

### **Step 5: Connecting to Source Database**

Each ETL opens a connection to the source database:

```csharp
// From appsettings.json
"SourceConnection": "Server=10.216.0.10,1480\\SQL2025;Database=All_Dataset;..."

// In ETL service constructor
_sourceConnectionString = configuration.GetConnectionString("SourceConnection");
```

---

### **Step 6: Extracting Data (The "E")**

Each ETL runs a specific SQL query to get its data:

#### **HTS ETL - HIV Testing Data**

```sql
SELECT
    FacilityCode, VisitDate, AgeGroup, SexName, PopulationGroup,
    ISNULL(HTS_TestedForHIV, 0) as HTS_TestedForHIV,
    ISNULL(HTS_TestedNegative, 0) as HTS_TestedNegative,
    ISNULL(HTS_TestedPositive, 0) as HTS_TestedPositive,
    ISNULL(HTS_TestedPositiveInitiatedOnART, 0) as HTS_TestedPositiveInitiatedOnART
FROM [All_Dataset].[dbo].[tmpHTSTestedDetail]
WHERE VisitDate IS NOT NULL
ORDER BY VisitDate
```

#### **PrEP ETL - Prevention Data (TWO sources)**

```sql
-- Primary source: LineListingsPrep
SELECT FacilityCode, VisitDate, AgeGroup, SexName, PopulationType,
       ISNULL(PrEP_Initiation, 0) as PrEP_Initiation,
       ISNULL(PrEP_TestedForHIV, 0) as PrEP_TestedForHIV,
       ISNULL(PrEP_TestedNegative, 0) as PrEP_TestedNegative,
       ISNULL(PrEP_TestedPositive, 0) as PrEP_TestedPositive,
       ISNULL(PrEP_InitiatedOnART, 0) as PrEP_InitiatedOnART
FROM [All_Dataset].[dbo].[LineListingsPrep]

-- Secondary source: aPrepDetail (for seroconversions)
SELECT FacilityCode, VisitDate, AgeGroup, Sex, PopulationType,
       Seroconverted, InitiatedOnART
FROM [All_Dataset].[dbo].[aPrepDetail]
```

#### **ART ETL - Treatment Data**

```sql
SELECT FacilityCode, ReportingPeriod, AgeGroup, SexName,
       ISNULL(TX_CURR, 0) as TX_CURR,
       ISNULL(TX_VLTested, 0) as TX_VLTested,
       ISNULL(TX_VLSuppressed, 0) as TX_VLSuppressed,
       ISNULL(TX_VLUndetectable, 0) as TX_VLUndetectable
FROM [All_Dataset].[dbo].[tmpARTTXOutcomes]
WHERE ReportingPeriod IS NOT NULL
ORDER BY ReportingPeriod DESC
```

---

### **Step 7: Transforming Data (The "T")**

For **each row** read from the source, the ETL does multiple transformations:

#### **A. Region Mapping (Using cached mappings)**

```csharp
var facilityRegions = await _facilityRegionService.GetFacilityRegionsAsync();

if (!facilityRegions.TryGetValue(facilityCode, out var regionId))
{
    unmappedFacilities.Add(facilityCode);
    continue; // Skip this record if facility can't be mapped
}
```

#### **B. Sex Standardization**

```csharp
var sex = sexName.ToUpper() switch
{
    "MALE" => "M",
    "FEMALE" => "F",
    _ => "Other"  // Handles any unexpected values
};
```

#### **C. Creating Multiple Indicator Records from One Source Row**

**Example: A single HTS test row creates up to 4 records:**

| Source Column                    | Value | Creates Indicator |
| -------------------------------- | ----- | ----------------- |
| HTS_TestedForHIV                 | 1     | `HTS_TST`         |
| HTS_TestedNegative               | 1     | `HTS_NEG`         |
| HTS_TestedPositive               | 0     | (nothing)         |
| HTS_TestedPositiveInitiatedOnART | 0     | (nothing)         |

**Result: 2 new records** (tested + negative result)

---

### **Step 8: Aggregation - The Secret Sauce**

Instead of storing every single row (millions of records), the ETL aggregates them:

```csharp
// ETLHelper.cs
public static Dictionary<string, int> AggregateRecords<T>(List<T> records)
{
    var aggregated = new Dictionary<string, int>();

    foreach (var record in records)
    {
        var key = CreateAggregationKey(
            record.Indicator,
            record.RegionId,
            record.VisitDate,
            record.AgeGroup,
            record.Sex,
            record.PopulationType
        );

        if (aggregated.ContainsKey(key))
            aggregated[key] += record.Value;
        else
            aggregated[key] = record.Value;
    }

    return aggregated;
}
```

**Example: 5 raw records become 1 aggregated record**

| Raw Records                 |     | Aggregated                       |
| --------------------------- | --- | -------------------------------- |
| M159, TX_CURR, Jan 15, M, 1 | →   | **M159, TX_CURR, Jan 15, M = 5** |
| M159, TX_CURR, Jan 15, M, 1 | →   |                                  |
| M159, TX_CURR, Jan 15, M, 1 | →   |                                  |
| M159, TX_CURR, Jan 15, M, 1 | →   |                                  |
| M159, TX_CURR, Jan 15, M, 1 | →   |                                  |

**Why this matters**: Storage efficiency! 334,385 ART records become ~26,000 aggregated records.

---

### **Step 9: Batch Processing for Performance**

Instead of saving one record at a time (slow), records are collected in batches:

```csharp
if (allRawRecords.Count >= _batchSize)  // Default: 10,000 records
{
    var aggregated = ETLHelper.AggregateRecords(allRawRecords);
    // Add to final list
    allRawRecords.Clear();
}
```

This is **~100x faster** than individual inserts!

---

### **Step 10: Progress Reporting**

Every 10,000 records, you see progress in the logs:

```
[16:20:29 INF] Processed 10,000 raw ART records
[16:20:30 INF] Processed 20,000 raw ART records
[16:20:30 INF] Processed 30,000 raw ART records
```

---

### **Step 11: Final Summary**

After processing, you get a beautiful box showing exactly what happened:

```
╔══════════════════════════════════════════════════════════╗
║                    ART ETL SUMMARY                       ║
╠══════════════════════════════════════════════════════════╣
║  Raw Records Read:         334,385                         ║
║  Aggregated Insert:         26,642                         ║
║  Aggregated Update:              0                         ║
║  Unchanged Groups:               0                         ║
║  Time Elapsed:             63,791ms                         ║
╚══════════════════════════════════════════════════════════╝
```

---

## 📊 **What Each ETL Does in Detail**

### **HTS ETL (HIV Testing)**

```
Source: tmpHTSTestedDetail (94,968 rows)
Target: IndicatorValues_Prevention

One patient visit can create:
- HTS_TST (tested for HIV)
- HTS_NEG (tested negative)
- HTS_POS (tested positive)
- LINKAGE_ART (started ART after positive)

Total possible records from one visit: Up to 4
```

### **PrEP ETL (Prevention)**

```
Primary Source: LineListingsPrep (4,760 rows)
Secondary Source: aPrepDetail (198,835 rows)
Target: IndicatorValues_Prevention

Indicators created:
- PREP_NEW (started PrEP)
- PREP_TESTED (tested for HIV while on PrEP)
- PREP_NEG (tested negative)
- PREP_POS (tested positive - seroconversion)
- PREP_SEROCONVERSION (separate indicator for seroconversion)
- PREP_LINKAGE_ART (started ART)

The ETL combines BOTH sources, using aggregation to prevent duplication
```

### **ART ETL (Treatment)**

```
Source: tmpARTTXOutcomes (334,385 rows)
Target: IndicatorValues_HIV

Indicators created (quarterly data):
- TX_CURR (currently on ART)
- TX_VL_TESTED (had viral load test)
- TX_VL_SUPPRESSED (viral load suppressed)
- TX_VL_UNDETECTABLE (viral load undetectable)
```

---

## 🎯 **Monitoring ETL Status**

### **Get ETL Job Status (Admin Only)**

```powershell
$headers = @{
    "Authorization" = "Bearer $token"
}

# Get status for specific job
Invoke-RestMethod -Uri "http://localhost:5171/api/etl/status/HTS" -Headers $headers
Invoke-RestMethod -Uri "http://localhost:5171/api/etl/status/PrEP" -Headers $headers
Invoke-RestMethod -Uri "http://localhost:5171/api/etl/status/ART" -Headers $headers
```

### **Get ETL History**

```powershell
$headers = @{
    "Authorization" = "Bearer $token"
}

# Get all history
Invoke-RestMethod -Uri "http://localhost:5171/api/etl/history" -Headers $headers

# Filter by job
Invoke-RestMethod -Uri "http://localhost:5171/api/etl/history?jobName=HTS&limit=50" -Headers $headers
```

### **Get Last Run Times**

```powershell
$headers = @{
    "Authorization" = "Bearer $token"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/etl/last-runs" -Headers $headers
```

**Sample Response:**

```json
{
  "success": true,
  "data": {
    "HTS": {
      "sourceTable": "tmpHTSTestedDetail",
      "targetTable": "IndicatorValues_Prevention",
      "lastRunTime": "2026-03-06T07:44:12Z",
      "lastBatchId": "HTS_20260306_074212_UTC",
      "status": "completed",
      "recordCount": 18159
    },
    "PrEP": { ... },
    "ART": { ... }
  }
}
```

---

## 🔍 **How to Verify ETL Success**

### **Quick Verification Script (PowerShell)**

```powershell
# Check record counts after ETL
$query = @"
SELECT
    'HIV Indicators' as TableName,
    Indicator,
    COUNT(*) as RecordCount,
    SUM(Value) as TotalValue
FROM IndicatorValues_HIV
GROUP BY Indicator
UNION ALL
SELECT
    'Prevention Indicators',
    Indicator,
    COUNT(*),
    SUM(Value)
FROM IndicatorValues_Prevention
GROUP BY Indicator;
"@

# Save query to file
$query | Out-File -FilePath "verify.sql" -Encoding UTF8

# Run using sqlcmd (if installed)
sqlcmd -S "102.37.18.0,1433" -U sa -P "1!2@3#Abcd123!" -d EswatiniHealth_Staging -i verify.sql
```

### **Expected Totals After Successful Run**

```
HIV Indicators:
- TX_CURR: 199,623
- TX_VL_TESTED: 180,444
- TX_VL_SUPPRESSED: 164,354
- TX_VL_UNDETECTABLE: 155,707

Prevention Indicators:
- HTS_TST: 14,806
- HTS_POS: 57,991
- HTS_NEG: 13,532
- LINKAGE_ART: 631
- PREP_NEW: 1,239
- PREP_SEROCONVERSION: 36
```

---

## 🚀 **One-Click PowerShell ETL Script**

Save this as **Run-ETL.ps1**:

```powershell
# Run-ETL.ps1 - Complete ETL runner with status checking

param(
    [string]$BaseUrl = "http://localhost:5171",
    [string]$EtlKey = "simple-etl-key-2026"
)

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "🚀 Eswatini Health ETL Runner" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan

$etlHeaders = @{
    "X-ETL-Key" = $EtlKey
    "Content-Type" = "application/json"
}

$sources = @("hts", "prep", "art")

foreach ($source in $sources) {
    Write-Host "`n▶️ Running $source ETL..." -ForegroundColor Green

    try {
        $response = Invoke-RestMethod -Uri "$BaseUrl/api/etl/trigger?source=$source" `
            -Method Post `
            -Headers $etlHeaders `
            -Body "{}"

        if ($response.success) {
            Write-Host "✅ $source ETL completed successfully!" -ForegroundColor Green
            Write-Host "   Records Read: $($response.data.recordsRead)"
            Write-Host "   Records Inserted: $($response.data.recordsInserted)"
            Write-Host "   Duration: $($response.data.durationMs)ms"
        } else {
            Write-Host "❌ $source ETL failed: $($response.message)" -ForegroundColor Red
        }
    }
    catch {
        Write-Host "❌ Error running $source ETL: $_" -ForegroundColor Red
    }

    # Wait 10 seconds between jobs
    if ($source -ne $sources[-1]) {
        Write-Host "`n⏱️  Waiting 10 seconds before next ETL..." -ForegroundColor Yellow
        Start-Sleep -Seconds 10
    }
}

Write-Host "`n=========================================" -ForegroundColor Cyan
Write-Host "✅ All ETL jobs completed!" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
```

**Run it:**

```powershell
.\Run-ETL.ps1
```

---

## 📈 **What Each Number Means**

| Metric               | Source | Staging  | What it tells you                      |
| -------------------- | ------ | -------- | -------------------------------------- |
| **Records Read**     | 94,968 | N/A      | How many rows were in source           |
| **Records Inserted** | N/A    | 18,159   | How many aggregated records were added |
| **Records Updated**  | N/A    | 0        | How many existing records changed      |
| **Duration**         | N/A    | 44,814ms | How long it took (ms)                  |

**Key insight**: 94,968 raw records → 18,159 aggregated records means each staging record represents ~5.2 raw records on average.

---

## 🎯 **Troubleshooting Common Issues**

### **Issue 1: "The connection is already in a transaction"**

```powershell
# Fix: Restart the API
dotnet run
# Then try ETL again
```

### **Issue 2: Unmapped facilities warning**

```
Found 15 unmapped facilities in ART
```

**Fix**: Check `FacilityRegionService.cs` - first-letter mapping should handle these automatically

### **Issue 3: HTS discrepancy warnings**

```
Found 530 HTS discrepancies totaling 530 tests
```

**What this means**: Some test records have positive+negative != tested count
**Fix**: Configure `AlwaysIncludeHtsTest = true` in HTSETLService.cs

### **Issue 4: ETL hangs/never completes**

```powershell
# Check if API is running
Invoke-RestMethod -Uri "http://localhost:5171/health"

# Check logs
Get-Content -Path "logs/api-*.txt" -Tail 50
```

---

## 🎉 **Key Points**

1. **SAFE processing** - Data processed in memory first, then replaced in transaction
2. **Smart facility mapping** - Explicit mappings + first-letter fallback = no missing patients
3. **Aggregation** - 334K raw records → 26K staging records (90% reduction)
4. **Idempotent** - Running ETL multiple times gives same result
5. **Self-healing** - Old data corrected automatically
6. **Monitorable** - Status endpoints to check last runs
7. **Progress logging** - See what's happening in real-time
8. **Beautiful summaries** - Know exactly what changed
