Here's a comprehensive testing guide with all API endpoints, example requests, and expected responses:

## 📋 **Base URL**
```
http://localhost:5171
```

---

## 🔐 **Authentication Endpoints**

### 1. **Register a new user**
```bash
curl -X POST "http://localhost:5171/api/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "testuser@health.gov.sz",
    "name": "Test",
    "surname": "User",
    "password": "Test123!"
  }'
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

### 2. **Login**
```bash
curl -X POST "http://localhost:5171/api/auth/login" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "admin@health.gov.sz",
    "password": "AdminPass123!"
  }'
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
```bash
curl -X GET "http://localhost:5171/api/auth/me" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

---

## 📊 **Dashboard Endpoints**

### 4. **Get Dashboard Summary**
```bash
curl -X GET "http://localhost:5171/api/dashboard/summary" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# With specific date
curl -X GET "http://localhost:5171/api/dashboard/summary?asOfDate=2026-03-01" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

**Expected Response:**
```json
{
  "success": true,
  "data": {
    "asOfDate": "2026-03-04T00:00:00Z",
    "metrics": [
      {
        "indicator": "TX_CURR",
        "name": "Currently on ART",
        "value": 214884,
        "target": 220000,
        "percentageOfTarget": 97.7,
        "unit": "number",
        "trend": "up"
      }
    ],
    "charts": [],
    "lastUpdated": "2026-03-04T14:30:33Z"
  }
}
```

### 5. **Get HIV Dashboard**
```bash
curl -X GET "http://localhost:5171/api/dashboard/hiv" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# With specific date
curl -X GET "http://localhost:5171/api/dashboard/hiv?asOfDate=2026-03-01" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

### 6. **Get Prevention Dashboard**
```bash
curl -X GET "http://localhost:5171/api/dashboard/prevention" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# With date range
curl -X GET "http://localhost:5171/api/dashboard/prevention?startDate=2026-02-01&endDate=2026-02-28" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

---

## 📈 **Indicators Endpoints**

### 7. **Get All Available Indicators**
```bash
curl -X GET "http://localhost:5171/api/indicators/available" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

### 8. **Get Indicator Data**
```bash
# Get all indicators
curl -X GET "http://localhost:5171/api/indicators/data" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# Filter by specific indicators
curl -X GET "http://localhost:5171/api/indicators/data?indicators=TX_CURR,TX_NEW" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# With date range
curl -X GET "http://localhost:5171/api/indicators/data?startDate=2026-01-01&endDate=2026-03-01" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# Filter by region
curl -X GET "http://localhost:5171/api/indicators/data?regionId=1" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# Filter by demographic
curl -X GET "http://localhost:5171/api/indicators/data?ageGroup=25-29&sex=F" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# Chart format
curl -X GET "http://localhost:5171/api/indicators/data?format=chart" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

### 9. **Get Indicator Trends**
```bash
curl -X GET "http://localhost:5171/api/indicators/trends?indicators=TX_CURR,TX_NEW&startDate=2026-01-01&endDate=2026-03-01&periodType=monthly" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

---

## 🗺️ **Regions Endpoints**

### 10. **Get All Regions**
```bash
curl -X GET "http://localhost:5171/api/regions" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
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
```bash
curl -X GET "http://localhost:5171/api/targets" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# Filter by indicator
curl -X GET "http://localhost:5171/api/targets?indicator=TX_CURR" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# Filter by region
curl -X GET "http://localhost:5171/api/targets?regionId=1" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# Filter by year
curl -X GET "http://localhost:5171/api/targets?year=2026" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

### 12. **Get Specific Target**
```bash
curl -X GET "http://localhost:5171/api/targets/1" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

### 13. **Create Target** (Admin Only)
```bash
curl -X POST "http://localhost:5171/api/targets" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE" \
  -H "Content-Type: application/json" \
  -d '{
    "indicator": "TX_CURR",
    "regionId": null,
    "year": 2026,
    "targetValue": 220000,
    "targetType": "number",
    "notes": "National target for 2026"
  }'
```

### 14. **Get Target Summary**
```bash
curl -X GET "http://localhost:5171/api/targets/summary/TX_CURR?year=2026" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# With quarter
curl -X GET "http://localhost:5171/api/targets/summary/TX_CURR?year=2026&quarter=1" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

---

## 🔄 **ETL Endpoints**

### 15. **Trigger HTS ETL**
```bash
curl -X POST "http://localhost:5171/api/etl/trigger?source=hts" \
  -H "X-ETL-Key: simple-etl-key-2026" \
  -H "Content-Type: application/json" \
  -d "{}"
```

### 16. **Trigger PrEP ETL**
```bash
curl -X POST "http://localhost:5171/api/etl/trigger?source=prep" \
  -H "X-ETL-Key: simple-etl-key-2026" \
  -H "Content-Type: application/json" \
  -d "{}"
```

### 17. **Trigger ART ETL**
```bash
curl -X POST "http://localhost:5171/api/etl/trigger?source=art" \
  -H "X-ETL-Key: simple-etl-key-2026" \
  -H "Content-Type: application/json" \
  -d "{}"
```

### 18. **Get ETL Status** (Admin Only)
```bash
curl -X GET "http://localhost:5171/api/etl/status/HTS" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

curl -X GET "http://localhost:5171/api/etl/status/PrEP" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

curl -X GET "http://localhost:5171/api/etl/status/ART" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

### 19. **Get ETL History** (Admin Only)
```bash
curl -X GET "http://localhost:5171/api/etl/history" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# Filter by job
curl -X GET "http://localhost:5171/api/etl/history?jobName=HTS&limit=50" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

### 20. **Get Last Run Times** (Admin Only)
```bash
curl -X GET "http://localhost:5171/api/etl/last-runs" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

---

## 👥 **User Endpoints**

### 21. **Get All Users** (Admin Only)
```bash
curl -X GET "http://localhost:5171/api/users" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"

# Filter active users
curl -X GET "http://localhost:5171/api/users?active=true" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

### 22. **Get Current User**
```bash
curl -X GET "http://localhost:5171/api/users/me" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

---

## 🏥 **Health Check** (Public)

### 23. **Health Check**
```bash
curl -X GET "http://localhost:5171/health"
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

## 📝 **Complete Test Script**

Save this as `test-api.sh`:

```bash
#!/bin/bash

# Configuration
BASE_URL="http://localhost:5171"
ETL_KEY="simple-etl-key-2026"

echo "========================================="
echo "Eswatini Health API Test Script"
echo "========================================="

# 1. Health Check
echo -e "\n1. Testing Health Check..."
curl -s -X GET "$BASE_URL/health" | jq '.'

# 2. Register a test user
echo -e "\n2. Registering test user..."
curl -s -X POST "$BASE_URL/api/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "tester@health.gov.sz",
    "name": "Test",
    "surname": "Tester",
    "password": "Test123!"
  }' | jq '.'

# 3. Login to get token
echo -e "\n3. Logging in as admin..."
TOKEN=$(curl -s -X POST "$BASE_URL/api/auth/login" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "admin@health.gov.sz",
    "password": "AdminPass123!"
  }' | jq -r '.data.token')

echo "Token: $TOKEN"

# 4. Get regions
echo -e "\n4. Getting regions..."
curl -s -X GET "$BASE_URL/api/regions" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

# 5. Get available indicators
echo -e "\n5. Getting available indicators..."
curl -s -X GET "$BASE_URL/api/indicators/available" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

# 6. Get dashboard summary
echo -e "\n6. Getting dashboard summary..."
curl -s -X GET "$BASE_URL/api/dashboard/summary" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

# 7. Trigger HTS ETL
echo -e "\n7. Triggering HTS ETL..."
curl -s -X POST "$BASE_URL/api/etl/trigger?source=hts" \
  -H "X-ETL-Key: $ETL_KEY" \
  -H "Content-Type: application/json" \
  -d "{}" | jq '.'

echo -e "\n========================================="
echo "Tests completed!"
echo "========================================="
```

Make it executable:
```bash
chmod +x test-api.sh
```

Run it:
```bash
./test-api.sh
```

## 📊 **Postman Collection**

You can also import this into Postman:

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

Set environment variable:
- `base_url`: `http://localhost:5171`
- `token`: (your JWT token after login)

This covers all your API endpoints with working examples! 🚀
