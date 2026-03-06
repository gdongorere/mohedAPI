# 📋 **Complete API Testing Guide**

## 📋 **Base URL**

```
http://localhost:5171
```

---

## 🔐 **Authentication Endpoints**

### 1. **Register a new user**

```powershell
$body = @{
    email = "testuser@health.gov.sz"
    name = "Test"
    surname = "User"
    password = "Test123!"
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:5171/api/auth/register" `
    -Method Post `
    -ContentType "application/json" `
    -Body $body
```

**Expected Response:**

```json
{
  "success": true,
  "message": "Registration successful. You can now login.",
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "email": "testuser@health.gov.sz",
    "name": "Test",
    "surname": "User",
    "role": "viewer",
    "isActive": true
  }
}
```

### 2. **Login and Save Token**

```powershell
$loginBody = @{
    email = "admin@health.gov.sz"
    password = "AdminPass123!"
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:5171/api/auth/login" `
    -Method Post `
    -ContentType "application/json" `
    -Body $loginBody

$token = $response.data.token
Write-Host "Token: $token"

# Save token for later use
$env:TOKEN = $token
```

**Save the token from response:**

```json
{
  "success": true,
  "message": "Login successful",
  "data": {
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "expiresIn": 86400,
    "user": {
      "id": "admin-id",
      "email": "admin@health.gov.sz",
      "name": "System",
      "surname": "Administrator",
      "role": "admin",
      "isActive": true
    }
  }
}
```

### 3. **Get Current User**

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/users/me" `
    -Method Get `
    -Headers $headers
```

---

## 📊 **Dashboard Endpoints**

### 4. **Get Dashboard Summary**

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/dashboard/summary" `
    -Method Get `
    -Headers $headers

# With specific date
Invoke-RestMethod -Uri "http://localhost:5171/api/dashboard/summary?asOfDate=2026-03-01" `
    -Method Get `
    -Headers $headers
```

### 5. **Get HIV Dashboard**

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/dashboard/hiv" `
    -Method Get `
    -Headers $headers

# With specific date
Invoke-RestMethod -Uri "http://localhost:5171/api/dashboard/hiv?asOfDate=2026-03-01" `
    -Method Get `
    -Headers $headers
```

### 6. **Get Prevention Dashboard**

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/dashboard/prevention" `
    -Method Get `
    -Headers $headers

# With date range
Invoke-RestMethod -Uri "http://localhost:5171/api/dashboard/prevention?startDate=2026-02-01&endDate=2026-02-28" `
    -Method Get `
    -Headers $headers
```

---

## 📈 **Indicators Endpoints**

### 7. **Get All Available Indicators**

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/indicators/available" `
    -Method Get `
    -Headers $headers
```

### 8. **Get Indicator Data**

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

# Get all indicators
Invoke-RestMethod -Uri "http://localhost:5171/api/indicators/data" `
    -Method Get `
    -Headers $headers

# Filter by specific indicators
Invoke-RestMethod -Uri "http://localhost:5171/api/indicators/data?indicators=TX_CURR,TX_NEW" `
    -Method Get `
    -Headers $headers

# With date range
Invoke-RestMethod -Uri "http://localhost:5171/api/indicators/data?startDate=2026-01-01&endDate=2026-03-01" `
    -Method Get `
    -Headers $headers

# Filter by region
Invoke-RestMethod -Uri "http://localhost:5171/api/indicators/data?regionId=1" `
    -Method Get `
    -Headers $headers

# Filter by demographic
Invoke-RestMethod -Uri "http://localhost:5171/api/indicators/data?ageGroup=25-29&sex=F" `
    -Method Get `
    -Headers $headers
```

### 9. **Get Indicator Trends**

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/indicators/trends?indicators=TX_CURR,TX_NEW&startDate=2026-01-01&endDate=2026-03-01&periodType=monthly" `
    -Method Get `
    -Headers $headers
```

---

## 🗺️ **Regions Endpoints**

### 10. **Get All Regions**

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/regions" `
    -Method Get `
    -Headers $headers
```

**Expected Response:**

```json
{
  "success": true,
  "count": 4,
  "data": [
    { "id": 1, "name": "Hhohho", "code": "HH" },
    { "id": 2, "name": "Manzini", "code": "MN" },
    { "id": 3, "name": "Shiselweni", "code": "SH" },
    { "id": 4, "name": "Lubombo", "code": "LB" }
  ]
}
```

---

## 🎯 **Targets Endpoints** (Admin Only)

### 11. **Get All Targets**

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/targets" `
    -Method Get `
    -Headers $headers

# Filter by indicator
Invoke-RestMethod -Uri "http://localhost:5171/api/targets?indicator=TX_CURR" `
    -Method Get `
    -Headers $headers

# Filter by region
Invoke-RestMethod -Uri "http://localhost:5171/api/targets?regionId=1" `
    -Method Get `
    -Headers $headers

# Filter by year
Invoke-RestMethod -Uri "http://localhost:5171/api/targets?year=2026" `
    -Method Get `
    -Headers $headers
```

### 12. **Get Specific Target**

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/targets/1" `
    -Method Get `
    -Headers $headers
```

### 13. **Create Target** (Admin Only)

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
    "Content-Type" = "application/json"
}

$targetBody = @{
    indicator = "TX_CURR"
    regionId = $null
    year = 2026
    targetValue = 220000
    targetType = "number"
    notes = "National target for 2026"
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:5171/api/targets" `
    -Method Post `
    -Headers $headers `
    -Body $targetBody
```

### 14. **Get Target Summary**

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/targets/summary/TX_CURR?year=2026" `
    -Method Get `
    -Headers $headers

# With quarter
Invoke-RestMethod -Uri "http://localhost:5171/api/targets/summary/TX_CURR?year=2026&quarter=1" `
    -Method Get `
    -Headers $headers
```

---

## 🔄 **ETL Endpoints**

### 15. **Trigger HTS ETL**

```powershell
$etlHeaders = @{
    "X-ETL-Key" = "simple-etl-key-2026"
    "Content-Type" = "application/json"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/etl/trigger?source=hts" `
    -Method Post `
    -Headers $etlHeaders `
    -Body "{}"
```

### 16. **Trigger PrEP ETL**

```powershell
$etlHeaders = @{
    "X-ETL-Key" = "simple-etl-key-2026"
    "Content-Type" = "application/json"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/etl/trigger?source=prep" `
    -Method Post `
    -Headers $etlHeaders `
    -Body "{}"
```

### 17. **Trigger ART ETL**

```powershell
$etlHeaders = @{
    "X-ETL-Key" = "simple-etl-key-2026"
    "Content-Type" = "application/json"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/etl/trigger?source=art" `
    -Method Post `
    -Headers $etlHeaders `
    -Body "{}"
```

### 18. **Get ETL Status** (Admin Only)

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/etl/status/HTS" `
    -Method Get `
    -Headers $headers

Invoke-RestMethod -Uri "http://localhost:5171/api/etl/status/PrEP" `
    -Method Get `
    -Headers $headers

Invoke-RestMethod -Uri "http://localhost:5171/api/etl/status/ART" `
    -Method Get `
    -Headers $headers
```

### 19. **Get ETL History** (Admin Only)

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/etl/history" `
    -Method Get `
    -Headers $headers

# Filter by job
Invoke-RestMethod -Uri "http://localhost:5171/api/etl/history?jobName=HTS&limit=50" `
    -Method Get `
    -Headers $headers
```

### 20. **Get Last Run Times** (Admin Only)

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/etl/last-runs" `
    -Method Get `
    -Headers $headers
```

---

## 👥 **User Endpoints**

### 21. **Get All Users** (Admin Only)

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/users" `
    -Method Get `
    -Headers $headers

# Filter active users
Invoke-RestMethod -Uri "http://localhost:5171/api/users?active=true" `
    -Method Get `
    -Headers $headers
```

### 22. **Get Current User**

```powershell
$headers = @{
    "Authorization" = "Bearer $env:TOKEN"
}

Invoke-RestMethod -Uri "http://localhost:5171/api/users/me" `
    -Method Get `
    -Headers $headers
```

---

## 🏥 **Health Check** (Public)

### 23. **Health Check**

```powershell
Invoke-RestMethod -Uri "http://localhost:5171/health" -Method Get
```

**Expected Response:**

```json
{
  "status": "healthy",
  "timestamp": "2026-03-04T14:30:33Z",
  "version": "1.0.0",
  "database": "EswatiniHealth_Staging"
}
```

---

## 📝 **Complete PowerShell Test Script**

Save this as **Test-Api.ps1**:

```powershell
# Test-Api.ps1 - Complete API Test Script for PowerShell

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Eswatini Health API Test Script" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan

$BASE_URL = "http://localhost:5171"
$ETL_KEY = "simple-etl-key-2026"

# Function to pretty print JSON
function Write-JsonResponse($response) {
    $response | ConvertTo-Json -Depth 10
}

# 1. Health Check
Write-Host "`n1. Testing Health Check..." -ForegroundColor Green
$health = Invoke-RestMethod -Uri "$BASE_URL/health" -Method Get
Write-JsonResponse $health

# 2. Register a test user
Write-Host "`n2. Registering test user..." -ForegroundColor Green
$registerBody = @{
    email = "tester@health.gov.sz"
    name = "Test"
    surname = "Tester"
    password = "Test123!"
} | ConvertTo-Json

try {
    $register = Invoke-RestMethod -Uri "$BASE_URL/api/auth/register" `
        -Method Post `
        -ContentType "application/json" `
        -Body $registerBody
    Write-JsonResponse $register
} catch {
    Write-Host "User might already exist: $_" -ForegroundColor Yellow
}

# 3. Login to get token
Write-Host "`n3. Logging in as admin..." -ForegroundColor Green
$loginBody = @{
    email = "admin@health.gov.sz"
    password = "AdminPass123!"
} | ConvertTo-Json

$loginResponse = Invoke-RestMethod -Uri "$BASE_URL/api/auth/login" `
    -Method Post `
    -ContentType "application/json" `
    -Body $loginBody

$token = $loginResponse.data.token
Write-Host "Token: $token" -ForegroundColor Yellow
$global:TOKEN = $token

$authHeaders = @{
    "Authorization" = "Bearer $token"
}

# 4. Get regions
Write-Host "`n4. Getting regions..." -ForegroundColor Green
$regions = Invoke-RestMethod -Uri "$BASE_URL/api/regions" `
    -Method Get `
    -Headers $authHeaders
Write-JsonResponse $regions

# 5. Get available indicators
Write-Host "`n5. Getting available indicators..." -ForegroundColor Green
$indicators = Invoke-RestMethod -Uri "$BASE_URL/api/indicators/available" `
    -Method Get `
    -Headers $authHeaders
Write-JsonResponse $indicators

# 6. Get dashboard summary
Write-Host "`n6. Getting dashboard summary..." -ForegroundColor Green
$dashboard = Invoke-RestMethod -Uri "$BASE_URL/api/dashboard/summary" `
    -Method Get `
    -Headers $authHeaders
Write-JsonResponse $dashboard

# 7. Trigger HTS ETL
Write-Host "`n7. Triggering HTS ETL..." -ForegroundColor Green
$etlHeaders = @{
    "X-ETL-Key" = $ETL_KEY
    "Content-Type" = "application/json"
}

$etlResult = Invoke-RestMethod -Uri "$BASE_URL/api/etl/trigger?source=hts" `
    -Method Post `
    -Headers $etlHeaders `
    -Body "{}"
Write-JsonResponse $etlResult

Write-Host "`n=========================================" -ForegroundColor Cyan
Write-Host "Tests completed!" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
```

### How to run the PowerShell script:

1. **Save the script** as `Test-Api.ps1`
2. **Open PowerShell** as Administrator
3. **Run the script:**

```powershell
# Navigate to the script directory
cd C:\path\to\your\script

# Run the script
.\Test-Api.ps1

# If you get execution policy error, run:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
# Then run the script again
```

### One-liner commands for quick testing:

```powershell
# Login and capture token
$token = (Invoke-RestMethod -Uri "http://localhost:5171/api/auth/login" -Method Post -ContentType "application/json" -Body '{"email":"admin@health.gov.sz","password":"AdminPass123!"}').data.token

# Use token for subsequent calls
$headers = @{Authorization = "Bearer $token"}
Invoke-RestMethod -Uri "http://localhost:5171/api/dashboard/summary" -Headers $headers

# Trigger ETL
$etlHeaders = @{"X-ETL-Key" = "simple-etl-key-2026"}
Invoke-RestMethod -Uri "http://localhost:5171/api/etl/trigger?source=hts" -Method Post -Headers $etlHeaders -Body "{}"
```

---

## 📊 **Postman Collection**

You can import this into Postman:

```json
{
  "info": {
    "name": "Eswatini Health API",
    "schema": "https://schema.getpostman.com/json/collection/v2.1.0/"
  },
  "item": [
    {
      "name": "Auth",
      "item": [
        {
          "name": "Register",
          "request": {
            "method": "POST",
            "url": "{{base_url}}/api/auth/register",
            "header": ["Content-Type: application/json"],
            "body": {
              "mode": "raw",
              "raw": "{\"email\":\"test@health.gov.sz\",\"name\":\"Test\",\"surname\":\"User\",\"password\":\"Test123!\"}"
            }
          }
        },
        {
          "name": "Login",
          "request": {
            "method": "POST",
            "url": "{{base_url}}/api/auth/login",
            "header": ["Content-Type: application/json"],
            "body": {
              "mode": "raw",
              "raw": "{\"email\":\"admin@health.gov.sz\",\"password\":\"AdminPass123!\"}"
            }
          }
        }
      ]
    }
  ]
}
```

Set environment variables in Postman:

- `base_url`: `http://localhost:5171`
- `token`: (your JWT token after login, set automatically using Tests tab)
