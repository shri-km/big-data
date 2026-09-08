#!/bin/bash

#  Install Java and Download Kafka

# 1. Install Java 17
echo "Starting system update and Java installation..."
sudo apt update -y
sudo apt install openjdk-17-jdk -y

# Verify Java
echo "Java installation complete. Checking version..."
java -version

# 2. Define Variables
KAFKA_VERSION="4.1.1"
SCALA_VERSION="2.13"
KAFKA_ARCHIVE="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
KAFKA_URL="https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"
INSTALL_DIR="$HOME/kafka" 

# 3. Download and Extract Kafka
echo "Downloading Kafka version ${KAFKA_VERSION}..."
wget -q $KAFKA_URL

if [ $? -ne 0 ]; then
    echo "Error: Failed to download Kafka from $KAFKA_URL"
    exit 1
fi

# Create the final directory
mkdir -p $INSTALL_DIR

echo "Extracting Kafka and moving to final location: $INSTALL_DIR"
# The --strip-components 1 flag extracts the contents *inside* the main folder, 
# rather than creating a nested folder. No sudo needed!
tar -xzf $KAFKA_ARCHIVE --directory $INSTALL_DIR --strip-components 1

# Clean up download
rm $KAFKA_ARCHIVE

echo "Kafka binaries are now installed in $INSTALL_DIR"
echo "Setup script finished. Proceed to the configuration script."