#!/bin/bash

# Distributed Health Check Script (TEMP)
# 모든 서비스의 헬스체크를 수행합니다.

set -e

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# .env 파일 로드
if [ -f "config/.env" ]; then
    export $(cat config/.env | grep -v '^#' | xargs)
else
    echo -e "${RED}❌ config/.env 파일을 찾을 수 없습니다${NC}"
    exit 1
fi

echo "======================================"
echo "   Distributed Health Check"
echo "======================================"
echo ""

# 헬스체크 함수
check_http() {
    local name=$1
    local url=$2
    local timeout=${3:-3}
    
    if curl -s --max-time $timeout "$url" > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} $name - OK"
        return 0
    else
        echo -e "${RED}✗${NC} $name - FAIL ($url)"
        return 1
    fi
}

check_http_with_output() {
    local name=$1
    local url=$2
    local timeout=${3:-3}
    
    response=$(curl -s --max-time $timeout "$url" 2>&1)
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $name - OK"
        return 0
    else
        echo -e "${RED}✗${NC} $name - FAIL ($url)"
        return 1
    fi
}

total=0
passed=0

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🗄️  Infrastructure"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Kafka
((total++))
if check_http "Kafka-1" "http://${KAFKA_1_IP}:${KAFKA_1_EXTERNAL_PORT}" 5; then ((passed++)); fi

((total++))
if check_http "Kafka-2" "http://${KAFKA_2_IP}:${KAFKA_2_EXTERNAL_PORT}" 5; then ((passed++)); fi

((total++))
if check_http "Kafka-3" "http://${KAFKA_3_IP}:${KAFKA_3_EXTERNAL_PORT}" 5; then ((passed++)); fi

# ClickHouse
((total++))
if check_http "ClickHouse" "http://${CLICKHOUSE_IP}:${CLICKHOUSE_HTTP_PORT}/ping" 5; then ((passed++)); fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🚀 Data Pipeline"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Nginx LB
((total++))
if check_http "Nginx LB" "http://${NGINX_IP}:${NGINX_LB_PORT}/health" 3; then ((passed++)); fi

# Ingestor
((total++))
if check_http "Ingestor-1" "http://${INGESTOR_1_IP}:${INGESTOR_1_PORT}/health" 3; then ((passed++)); fi

((total++))
if check_http "Ingestor-2" "http://${INGESTOR_2_IP}:${INGESTOR_2_PORT}/health" 3; then ((passed++)); fi

((total++))
if check_http "Ingestor-3" "http://${INGESTOR_3_IP}:${INGESTOR_3_PORT}/health" 3; then ((passed++)); fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 Monitoring"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Loki
((total++))
if check_http "Loki" "http://${LOKI_IP}:${LOKI_PORT}/ready" 3; then ((passed++)); fi

# Prometheus (LOKI_IP와 동일한 머신에서 실행)
((total++))
if check_http "Prometheus" "http://${LOKI_IP}:${PROMETHEUS_PORT}/-/healthy" 3; then ((passed++)); fi

# Grafana (LOKI_IP와 동일한 머신에서 실행)
((total++))
if check_http "Grafana" "http://${LOKI_IP}:${GRAFANA_PORT}/api/health" 3; then ((passed++)); fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔧 Processing"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Flink (ClickHouse 연동 필수)
((total++))
if check_http "Flink JobManager" "http://${FLINK_IP}:${FLINK_JOBMANAGER_PORT}/overview" 3; then ((passed++)); fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📈 Summary"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "Total: $total"
echo -e "Passed: ${GREEN}$passed${NC}"
echo -e "Failed: ${RED}$((total - passed))${NC}"
echo ""

# 성공률 계산
success_rate=$((passed * 100 / total))

if [ $success_rate -eq 100 ]; then
    echo -e "${GREEN}🎉 All services are healthy!${NC}"
    exit 0
elif [ $success_rate -ge 80 ]; then
    echo -e "${YELLOW}⚠️  Most services are healthy ($success_rate%)${NC}"
    exit 0
else
    echo -e "${RED}❌ Many services are down ($success_rate%)${NC}"
    exit 1
fi
