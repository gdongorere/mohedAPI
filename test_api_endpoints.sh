#!/bin/bash

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

BASE_URL="http://localhost:5171"
TOKEN="YOUR_TOKEN_HERE"  # Replace with actual token after login

echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}🔍 API DATA VERIFICATION${NC}"
echo -e "${BLUE}=================================================================${NC}"
echo ""

# First login to get token (uncomment and run once)
echo -e "${YELLOW}Logging in to get token...${NC}"
TOKEN=$(curl -s -X POST "$BASE_URL/api/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@health.gov.sz","password":"AdminPass123!"}' | jq -r '.data.token')
echo -e "${GREEN}✅ Token obtained${NC}"
echo ""

# ============================================
# 1. HTS DATA TESTS
# ============================================
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 1. HTS DATA TESTS${NC}"
echo -e "${BLUE}=================================================================${NC}"

# Get HTS_TST totals
echo -e "${YELLOW}HTS_TST (Total Tests):${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=HTS_TST" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${YELLOW}HTS_TST by Region:${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=HTS_TST" \
  -H "Authorization: Bearer $TOKEN" | jq '.data | group_by(.RegionId) | map({region: .[0].RegionId, total: map(.Value) | add})'

echo -e "\n${YELLOW}HTS_TST by Age Group (last 30 days):${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=HTS_TST&startDate=2026-02-01&endDate=2026-03-01" \
  -H "Authorization: Bearer $TOKEN" | jq '.data | group_by(.AgeGroup) | map({age: .[0].AgeGroup, total: map(.Value) | add})'

echo -e "\n${YELLOW}HTS_POS (Positives):${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=HTS_POS" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${YELLOW}HTS_NEG (Negatives):${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=HTS_NEG" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${YELLOW}LINKAGE_ART (Linked to ART):${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=LINKAGE_ART" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

# ============================================
# 2. PrEP DATA TESTS
# ============================================
echo -e "\n${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 2. PrEP DATA TESTS${NC}"
echo -e "${BLUE}=================================================================${NC}"

echo -e "${YELLOW}PREP_NEW (Initiations):${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=PREP_NEW" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${YELLOW}PREP_SEROCONVERSION (Seroconversions):${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=PREP_SEROCONVERSION" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${YELLOW}PREP_LINKAGE_ART (Linked to ART):${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=PREP_LINKAGE_ART" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${YELLOW}PrEP by Method:${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=PREP_NEW&format=chart" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

# ============================================
# 3. ART DATA TESTS
# ============================================
echo -e "\n${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 3. ART DATA TESTS${NC}"
echo -e "${BLUE}=================================================================${NC}"

echo -e "${YELLOW}TX_CURR (Currently on ART):${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=TX_CURR" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${YELLOW}TX_VL_TESTED (Viral Load Tested):${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=TX_VL_TESTED" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${YELLOW}TX_VL_SUPPRESSED (Suppressed):${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=TX_VL_SUPPRESSED" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${YELLOW}TX_VL_UNDETECTABLE (Undetectable):${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=TX_VL_UNDETECTABLE" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${YELLOW}ART Dashboard Summary:${NC}"
curl -s -X GET "$BASE_URL/api/dashboard/hiv" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

# ============================================
# 4. REGIONAL BREAKDOWNS
# ============================================
echo -e "\n${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 4. REGIONAL BREAKDOWNS${NC}"
echo -e "${BLUE}=================================================================${NC}"

echo -e "${YELLOW}All Regions:${NC}"
curl -s -X GET "$BASE_URL/api/regions" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${YELLOW}HTS by Region (Hhohho - RegionId=1):${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=HTS_TST,HTS_POS&regionId=1" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${YELLOW}ART by Region (Manzini - RegionId=2):${NC}"
curl -s -X GET "$BASE_URL/api/indicators/data?indicators=TX_CURR&regionId=2" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

# ============================================
# 5. DASHBOARD SUMMARIES
# ============================================
echo -e "\n${BLUE}=================================================================${NC}"
echo -e "${BLUE}📊 5. DASHBOARD SUMMARIES${NC}"
echo -e "${BLUE}=================================================================${NC}"

echo -e "${YELLOW}Main Dashboard Summary:${NC}"
curl -s -X GET "$BASE_URL/api/dashboard/summary" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${YELLOW}Prevention Dashboard:${NC}"
curl -s -X GET "$BASE_URL/api/dashboard/prevention" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${YELLOW}HIV Dashboard:${NC}"
curl -s -X GET "$BASE_URL/api/dashboard/hiv" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

echo -e "\n${BLUE}=================================================================${NC}"
echo -e "${GREEN}✅ API Tests Complete${NC}"
echo -e "${BLUE}=================================================================${NC}"