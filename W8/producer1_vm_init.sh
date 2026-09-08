sudo apt-get update
sudo apt-get install -y python3-pip python3-venv git

python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip
pip install kafka-python google-cloud-storage

BROKER_IP="${KAFKA_VM_INTERNAL_IP:-10.160.15.206}"
BROKER_PORT="${KAFKA_PORT:-9092}"
echo "Pinging Kafka Broker at ${BROKER_IP}:${BROKER_PORT}..."
timeout 3 bash -c "</dev/tcp/${BROKER_IP}/${BROKER_PORT}" 2>/dev/null && echo "Kafka Broker is REACHABLE (Port ${BROKER_PORT} is open)" || echo "Kafka Broker is UNREACHABLE (Connection failed or timed out)"