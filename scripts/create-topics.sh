echo "Waiting for Kafka to come online..."

cub kafka-ready -b kafka:9092 1 20

# create the users topic
kafka-topics \
  --bootstrap-server kafka:9092 \
  --topic bank-transactions \
  --replication-factor 1 \
  --partitions 4 \
  --create

kafka-topics \
  --bootstrap-server kafka:9092 \
  --topic bank-balances \
  --replication-factor 1 \
  --partitions 4 \
  --create


kafka-topics \
  --bootstrap-server kafka:9092 \
  --topic rejected-transactions \
  --replication-factor 1 \
  --partitions 4 \
  --create

sleep infinity
