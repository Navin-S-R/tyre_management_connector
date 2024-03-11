#!/bin/bash

# Stop MongoDB
sudo systemctl stop mongod

# Purge MongoDB
sudo apt-get purge mongodb-org* -y

# Remove MongoDB log directory
sudo rm -r /var/log/mongodb
sudo rm -r /var/lib/mongodb

# Install dependencies
sudo apt-get install gnupg curl -y

# Add MongoDB GPG key
curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
   sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg \
   --dearmor -y

# Add MongoDB repository
echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list -y

# Update package lists
sudo apt-get update

# Install MongoDB
sudo apt-get install -y mongodb-org

# Start MongoDB
sudo systemctl start mongod

# Reload systemd
sudo systemctl daemon-reload

# Stop MongoDB
sudo systemctl stop mongod

# Restart MongoDB
sudo systemctl restart mongod

sudo systemctl enable mongod

# Run mongosh commands
mongosh <<EOF
use admin
db.createUser({ user: "root", pwd: "root", roles: [{ role: "root", db: "admin" }] })
exit
EOF
