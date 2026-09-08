#!/bin/bash

sudo apt update && \
sudo apt install -y python3 python3-venv python3-pip

python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip
pip install google-cloud-storage google-cloud-pubsub