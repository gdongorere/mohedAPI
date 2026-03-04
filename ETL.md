# 📚 **ETL Explained in Simple English**

## 🏥 **Our ETL System Overview**

We have **3 ETL services**, each handling different types of health data:

| ETL Service | Source Tables | Destination Table | What it does |
|-------------|---------------|-------------------|--------------|
| **HTSETLService** | `tmpHTSTestedDetail` | `IndicatorValues_Prevention` | Processes HIV testing data |
| **PrEPETLService** | `LineListingsPrep` + `aPrepDetail` | `IndicatorValues_Prevention` | Processes PrEP (prevention) data |
| **ARTETLService** | `tmpARTTXOutcomes` | `IndicatorValues_HIV` | Processes ART (treatment) data |

---

## 🔄 **The ETL Process - Step by Step**

### **Step 1: Trigger the ETL**
When you run this command:
```bash
curl -X POST "http://localhost:5171/api/etl/trigger?source=hts" -H "X-ETL-Key: simple-etl-key-2026"
```

The system:
1. Checks if your API key is valid (`simple-etl-key-2026`)
2. Routes to the correct ETL service (`HTSETLService` for "hts")
3. Creates a unique **Batch ID** like `HTS_20260304_143029_UTC` (date and time stamp)

---

### **Step 2: Load Existing Records (Deduplication)**

Before processing new data, the ETL asks:
> "What data do we already have in our staging database?"

```csharp
var existingRecords = await ETLHelper.LoadExistingRecordsAsync<IndicatorValuePrevention>(_db, _logger, DateTime.UtcNow.AddDays(-90));
```

This loads all records from the last 90 days into memory as a **dictionary** with unique keys.

**How the unique key is created:**
```
[Indicator]|[RegionId]|[Date]|[AgeGroup]|[Sex]|[PopulationType]
Example: HTS_TST|1|2024-06-13|25-29|F|General Population
```

This key ensures we don't count the same thing twice!

---

### **Step 3: Connect to Source Database**

The ETL opens a connection to the source database (`All_Dataset`) using the connection string from `appsettings.json`:

```json
"SourceConnection": "Server=10.216.0.10,1480\\SQL2025;Database=All_Dataset;User Id=sa;Password=Support25;"
```

---

### **Step 4: Extract Data (The "E" in ETL)**

The ETL runs a SQL query to get raw data. For HTS, it's:

```sql
SELECT 
    FacilityCode,
    VisitDate,
    AgeGroup,
    SexName,
    PopulationGroup,
    HTS_TestedForHIV,
    HTS_TestedNegative,
    HTS_TestedPositive,
    HTS_TestedPositiveInitiatedOnART
FROM [All_Dataset].[dbo].[tmpHTSTestedDetail]
WHERE VisitDate IS NOT NULL
ORDER BY VisitDate
```

This reads ALL records from the source table (no date filtering).

---

### **Step 5: Transform Data (The "T" in ETL)**

For each row read, the ETL does several transformations:

#### **A. Find the Region ID**
```csharp
// From aPrepDetail table, we map facility codes to region IDs
Facility "H001" → Region "Hhohho" → RegionId = 1
Facility "M020" → Region "Manzini" → RegionId = 2
```

#### **B. Clean up sex values**
```csharp
// Convert "Male" → "M", "Female" → "F", anything else → "Other"
var sex = sexName switch {
    "MALE" => "M",
    "FEMALE" => "F",
    _ => "Other"
};
```

#### **C. Create indicator records**
For each column that equals `1` (true), we create a new record:

**Example row from HTS:**
```
HTS_TestedForHIV = 1
HTS_TestedNegative = 1  
HTS_TestedPositive = 0
HTS_TestedPositiveInitiatedOnART = 0
```

This creates **2 records**:
```json
{
  "Indicator": "HTS_TST",
  "RegionId": 1,
  "VisitDate": "2024-06-13",
  "AgeGroup": "25-29",
  "Sex": "F",
  "PopulationType": "General Population",
  "Value": 1
}
{
  "Indicator": "HTS_NEG",
  "RegionId": 1,
  "VisitDate": "2024-06-13", 
  "AgeGroup": "25-29",
  "Sex": "F",
  "PopulationType": "General Population",
  "Value": 1
}
```

---

### **Step 6: Check for Duplicates (The "Don't Count Twice" Rule)**

For each potential new record, the ETL asks:
> "Have we seen this exact combination before?"

It creates a **unique key** from the record:
```
HTS_TST|1|2024-06-13|25-29|F|General Population
```

Then checks if this key exists in the `existingRecords` dictionary:

| Scenario | What happens |
|----------|--------------|
| **Key NOT found** | This is new data! Add to insert list |
| **Key FOUND + older** | This is a duplicate, skip it |
| **Key FOUND + newer** | This is an update, count it for later |

---

### **Step 7: Batch Insert (The "L" in ETL)**

Instead of saving one record at a time (slow), the ETL collects records in batches of 10,000:

```csharp
if (allRecords.Count >= _batchSize)  // 10,000 records
{
    await db.AddRangeAsync(allRecords);
    await db.SaveChangesAsync();
    allRecords.Clear();
}
```

This is much faster! Like filling a bathtub with buckets vs. teaspoons.

---

### **Step 8: Progress Reporting**

Every 10,000 records, you see a log:
```
[16:20:29 INF] Processed 10,000 aPrepDetail records
[16:20:30 INF] Processed 20,000 aPrepDetail records
[16:20:30 INF] Processed 30,000 aPrepDetail records
```

This lets you watch the ETL progress in real-time.

---

### **Step 9: Final Summary**

At the end, you get a beautiful box showing what happened:

```
╔══════════════════════════════════════════════════════════╗
║                    aPrepDetail SUMMARY                    ║
╠══════════════════════════════════════════════════════════╣
║  Records Read:          198,835                              ║
║  Records Inserted:         355                              ║
║  Duplicates Found:           0                              ║
║  Time Elapsed:            4,064ms                              ║
╚══════════════════════════════════════════════════════════╝
```

---

## 📊 **How Each ETL Creates Your Final Data**

### **HTS ETL** → `IndicatorValues_Prevention`

| Source Column | Becomes Indicator | When |
|---------------|-------------------|------|
| `HTS_TestedForHIV` | `HTS_TST` | When = 1 |
| `HTS_TestedNegative` | `HTS_NEG` | When = 1 |
| `HTS_TestedPositive` | `HTS_POS` | When = 1 |
| `HTS_TestedPositiveInitiatedOnART` | `LINKAGE_ART` | When = 1 |

**Example Journey:**
1. Patient visits clinic on 2024-06-13
2. Gets HIV test → `HTS_TestedForHIV = 1`
3. Tests negative → `HTS_TestedNegative = 1`
4. ETL creates: `HTS_TST` and `HTS_NEG` records
5. These are saved to `IndicatorValues_Prevention`

---

### **PrEP ETL** → `IndicatorValues_Prevention`

**From `LineListingsPrep` (Primary):**

| Source Column | Becomes Indicator | When |
|---------------|-------------------|------|
| `PrEP_Initiation` | `PREP_NEW` | When = 1 |
| `PrEP_TestedForHIV` | `PREP_TESTED` | When = 1 |
| `PrEP_TestedNegative` | `PREP_NEG` | When = 1 |
| `PrEP_TestedPositive` | `PREP_POS` + `PREP_SEROCONVERSION` | When = 1 |
| `PrEP_InitiatedOnART` | `PREP_LINKAGE_ART` | When = 1 |

**From `aPrepDetail` (Secondary - for seroconversions):**

| Source Column | Becomes Indicator | When |
|---------------|-------------------|------|
| `Seroconverted` | `PREP_SEROCONVERSION` | When = "1", "true", or "yes" |
| `InitiatedOnART` | `PREP_LINKAGE_ART` | When = "1", "true", or "yes" |

**Example Journey:**
1. Person starts PrEP → `PrEP_Initiation = 1` → `PREP_NEW` created
2. Months later, they test positive → `Seroconverted = "1"` in aPrepDetail → `PREP_SEROCONVERSION` created
3. They start ART → `InitiatedOnART = "1"` → `PREP_LINKAGE_ART` created

---

### **ART ETL** → `IndicatorValues_HIV`

| Source Column | Becomes Indicator | When |
|---------------|-------------------|------|
| `TX_CURR` | `TX_CURR` | When = 1 |
| `TX_VLTested` | `TX_VL_TESTED` | When = 1 |
| `TX_VLSuppressed` | `TX_VL_SUPPRESSED` | When = 1 |
| `TX_VLUndetectable` | `TX_VL_UNDETECTABLE` | When = 1 |

**Note:** ART data is quarterly (every 3 months), not daily.

---

## 🔍 **The Deduplication Magic**

The most important part of ETL is **not counting the same thing twice**.

### Example: A patient visits the clinic 3 times

| Visit Date | HTS_TestedForHIV | What should happen |
|------------|------------------|-------------------|
| Jan 15 | 1 | Create HTS_TST record |
| Feb 20 | 1 | Create HTS_TST record (different day = different record) |
| Feb 20 | 1 | ❌ SKIP - Same day would be duplicate |

The ETL uses this unique key to prevent duplicates:
```
HTS_TST|1|2024-02-20|25-29|F|General Population
```

If this exact combination already exists in the database, it's skipped!

---

## 📈 **From Raw Data to Dashboard**

1. **Raw Data** (Source DB): Millions of line-list rows
2. **ETL Process**: Transforms into clean indicator records
3. **Staging DB**: `IndicatorValues_Prevention` and `IndicatorValues_HIV` tables
4. **API Queries**: Your dashboard queries these tables
5. **Dashboard**: Shows beautiful charts and numbers!

### Example Dashboard Query:
```sql
-- Get total HIV tests in March 2024
SELECT SUM(Value) 
FROM IndicatorValues_Prevention
WHERE Indicator = 'HTS_TST'
  AND VisitDate BETWEEN '2024-03-01' AND '2024-03-31'
  AND RegionId = 1  -- Hhohho region
```

---

## 🎯 **Key Takeaways**

1. **ETL runs on-demand** - You trigger it when you want fresh data
2. **Deduplication prevents double-counting** - The same patient visit won't be counted twice
3. **Each source row can create multiple indicator records** - One HTS test creates both "tested" and "result" records
4. **Batch processing makes it fast** - 10,000 records at a time
5. **Progress logging keeps you informed** - You see what's happening in real-time
6. **Final summary tells you exactly what happened** - How many read vs. inserted vs. duplicate

The ETL is like a smart filter that takes messy, raw healthcare data and turns it into clean, organized indicators that your dashboard can easily understand and display!
