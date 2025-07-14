#!/bin/bash
set -e

echo "Checking if PM2 is already installed..."

# Check if PM2 is already installed
if command -v pm2 &> /dev/null; then
    echo "PM2 is already installed. Version: $(pm2 --version)"
    echo "Skipping PM2 installation..."
    exit 0
fi

echo "Installing Node.js and PM2..."

apt update
apt install -y curl gnupg

curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt install -y nodejs
node -v
npm -v

npm install -g pm2
pm2 startup

echo "Node.js and PM2 installation completed!" 
