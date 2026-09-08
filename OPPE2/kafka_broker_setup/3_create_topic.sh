cd kafka

# Create topic with 3 partitions
bin/kafka-topics.sh --create \
  --topic aqdata \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

echo "Topic Created. Check description"

# Describe topic to see details
bin/kafka-topics.sh --describe \
  --topic aqdata \
  --bootstrap-server localhost:9092