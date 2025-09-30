#!/bin/bash
set -e

# Configuration
KAFKA_BROKER="${KAFKA_BROKER:-kafka:29092}"
MAX_WAIT="${MAX_WAIT:-30}"
TOPICS_CONFIG="${TOPICS_CONFIG:-/usr/local/etc/topics.conf}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}    Kafka Topic Setup${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""

# Wait for Kafka to be ready
echo -e "${YELLOW}Waiting for Kafka to be ready at ${KAFKA_BROKER}...${NC}"
max_attempts=$MAX_WAIT
attempt=1

while [ $attempt -le $max_attempts ]; do
    echo -e "Attempt $attempt/$max_attempts..."
    if kafka-topics --list --bootstrap-server ${KAFKA_BROKER} >/dev/null 2>&1; then
        echo -e "${GREEN}✓ Kafka is ready!${NC}"
        break
    fi
    if [ $attempt -eq $max_attempts ]; then
        echo -e "${RED}ERROR: Kafka not ready after ${MAX_WAIT} attempts${NC}"
        exit 1
    fi
    attempt=$((attempt + 1))
    sleep 1
done
echo ""

# Read topics from configuration file
echo -e "${YELLOW}Reading topics from ${TOPICS_CONFIG}...${NC}"
echo ""

if [ ! -f "${TOPICS_CONFIG}" ]; then
    echo -e "${RED}ERROR: Topics configuration file not found at ${TOPICS_CONFIG}${NC}"
    exit 1
fi

# Create topics
echo -e "${YELLOW}Creating topics...${NC}"
echo ""

while IFS= read -r line; do
    # Skip empty lines and comments
    [ -z "$line" ] && continue
    [[ "$line" =~ ^#.*$ ]] && continue
    
    IFS=":" read -r topic partitions replication <<< "$line"
    topic=$(echo "$topic" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    partitions=$(echo "$partitions" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    replication=$(echo "$replication" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    
    echo -e "Creating topic: ${GREEN}${topic}${NC}"
    echo "  Partitions: ${partitions}"
    echo "  Replication Factor: ${replication}"
    
    kafka-topics --create \
        --if-not-exists \
        --topic "${topic}" \
        --partitions "${partitions}" \
        --replication-factor "${replication}" \
        --bootstrap-server "${KAFKA_BROKER}" 2>&1 | while read output_line; do
        if [[ $output_line == *"already exists"* ]]; then
            echo -e "  ${YELLOW}Topic already exists, skipping...${NC}"
        elif [[ $output_line == *"Created topic"* ]]; then
            echo -e "  ${GREEN}✓ Topic created successfully${NC}"
        fi
    done
    echo ""
done < "${TOPICS_CONFIG}"

# List all topics for verification
echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}    Verifying Topics${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""

echo -e "${YELLOW}All topics in Kafka:${NC}"
kafka-topics --list --bootstrap-server "${KAFKA_BROKER}" | while read topic_name; do
    echo -e "  ${GREEN}✓${NC} ${topic_name}"
done

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}    Topic Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"