#!/bin/bash

KAFKA_DIR="$HOME/kafka"
CONFIG_FILE="${KAFKA_DIR}/config/server.properties"
BROKER_ID=1

# 1. EDIT THESE VARIABLES WITH KAFKA VROKER VM'S IPs

VM_INTERNAL_IP="${KAFKA_VM_INTERNAL_IP:-10.160.15.206}"
VM_EXTERNAL_IP="${KAFKA_VM_EXTERNAL_IP:-34.180.45.18}"

if [ "$VM_INTERNAL_IP" == "<YOUR_VM_INTERNAL_IP>" ] || [ "$VM_EXTERNAL_IP" == "<YOUR_VM_EXTERNAL_IP>" ]; then
    echo "ERROR: Please edit this script and replace <YOUR_VM_INTERNAL_IP> and <YOUR_VM_EXTERNAL_IP> with your actual VM IPs."
    exit 1
fi

echo "Using Internal IP: $VM_INTERNAL_IP and External IP: $VM_EXTERNAL_IP"

# 2. Modify server.properties for KRaft and Network Configuration
echo "Modifying Kafka configuration file..."
cp $CONFIG_FILE ${CONFIG_FILE}.bak

# Set Broker ID
sed -i "s/^broker.id=.*/broker.id=${BROKER_ID}/" $CONFIG_FILE

# --- ROBUST SED REPLACEMENT ---
# Use an explicit replacement command (s/find/replace/) to handle the uncommented lines.

# Set Listeners: Bind to all interfaces (0.0.0.0 or just empty host) on 9092 and 9093
# Note: Using 0.0.0.0 for explicit binding to ensure the VM is listening on all IPs
sed -i "s|^listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093|" $CONFIG_FILE

# Set Advertised Listeners: This is what clients connect to (External IP)
sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://${VM_EXTERNAL_IP}:9092,CONTROLLER://${VM_INTERNAL_IP}:9093|" $CONFIG_FILE

VOTERS_LINE="controller.quorum.voters=${BROKER_ID}@${VM_INTERNAL_IP}:9093"
sed -i "\$a$VOTERS_LINE" $CONFIG_FILE

# 3. Initialize KRaft Storage (No sudo needed for files in $HOME)
echo "Initializing KRaft storage..."
# Ensure KRaft ID is only generated once if the script is run multiple times
if [ ! -f "${KAFKA_DIR}/.cluster_id" ]; then
    KAFKA_CLUSTER_ID=$($KAFKA_DIR/bin/kafka-storage.sh random-uuid)
    echo $KAFKA_CLUSTER_ID > ${KAFKA_DIR}/.cluster_id
else
    KAFKA_CLUSTER_ID=$(cat ${KAFKA_DIR}/.cluster_id)
fi

# Format storage only if it hasn't been done
if [ ! -d "${KAFKA_DIR}/data/kraft" ]; then
    echo "Formatting new KRaft storage directory..."
    $KAFKA_DIR/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c $CONFIG_FILE
else
    echo "KRaft storage already formatted. Skipping format step."
fi

# 4. Start Kafka Server (Using daemon mode for background running)
echo "Starting Kafka Server (KRaft mode) in daemon (background) mode."
echo "Use 'cd ~/kafka' then 'bin/kafka-server-stop.sh' to shut it down."
$KAFKA_DIR/bin/kafka-server-start.sh -daemon $CONFIG_FILE

echo "Kafka is running in the background. You can safely close this SSH window now."