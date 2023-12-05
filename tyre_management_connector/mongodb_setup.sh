#!/bin/bash

# Stop MongoDB
sudo service mongod stop

# Purge MongoDB
sudo apt-get purge mongodb-org* -y

# Remove MongoDB log directory
sudo rm -r /var/log/mongodb
sudo rm -r /var/lib/mongodb

# Install dependencies
sudo apt-get install gnupg curl -y

# Add MongoDB GPG key
curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | sudo gpg --dearmor -o /usr/share/keyrings/mongodb-archive-keyring.gpg -y

# Add MongoDB repository
echo "deb [signed-by=/usr/share/keyrings/mongodb-archive-keyring.gpg] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list -y

# Update package lists
sudo apt-get update

# Install MongoDB
sudo apt-get install -y mongodb-org

# Start MongoDB
sudo systemctl start mongod

# Reload systemd
sudo systemctl daemon-reload
sudo systemctl enable mongod

# Run mongosh commands
mongosh <<EOF
use admin
db.createUser({ user: "root", pwd: "root", roles: [{ role: "root", db: "admin" }] })
exit
EOF
