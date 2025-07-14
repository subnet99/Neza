# Neza Subnet Installation Guide

## Table of Contents

1. [Environment Requirements](#environment-requirements)
2. [Installation Steps](#installation-steps)
   - [Clone Repository](#clone-repository)
   - [Create Virtual Environment](#create-virtual-environment)
   - [Install Dependencies](#install-dependencies)
3. [Install ComfyUI](#install-comfyui)
4. [Install PostgreSQL Database](#install-postgresql-database)
5. [Set Environment Variables](#set-environment-variable)
6. [Validator Startup Command](#validator-startup-command)

## Environment Requirements

Before starting the installation, please ensure your system meets the following requirements:

### Validator Node

- CUDA 11.8 or higher
- Python 3.10 or higher
- NVIDIA driver version > 525
- Meet the [hardware requirements](../README.md#validator-recommended-configuration)

## Installation Steps

### Clone Repository

```bash
git clone https://github.com/subnet99/Neza.git
cd Neza
```

### Create Virtual Environment

It's recommended to use venv to create an isolated Python environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Install Dependencies

```bash
# Install PyTorch (choose the appropriate command based on your CUDA version)
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

# Install subnet dependencies
pip install -r requirements.txt

pip install -e .
```

## Install ComfyUI

- According to your GPU model, refer to [ComfyUI](https://github.com/comfyanonymous/ComfyUI) to prepare the system environment.
- Install pm2 (skip this step if already installed)

```
chmod +x ./scripts/install_pm2.sh
./scripts/install_pm2.sh
```

- Setup ComfyUI

If it has multiple GPUs, simply repeat run `setup_comfyui.sh` for each GPU

```
chmod +x ./scripts/setup_comfyui.sh
./scripts/setup_comfyui.sh
```

## Install PostgreSQL Database
You can refer to: https://www.postgresql.org/download/ or you can use the following command to install (the installation command may be slightly different for different operating systems, the following only takes Ubuntu 24.04 as an example)

```bash
# Update package lists
sudo apt update

# Install PostgreSQL
sudo apt install -y postgresql postgresql-contrib

# Enable and start service
sudo systemctl enable postgresql
sudo systemctl start postgresql

# Create database and change root user password
sudo -i -u postgres psql <<EOF
ALTER USER postgres WITH ENCRYPTED PASSWORD 'YOUR_POSTGRES_PASSWORD';
CREATE DATABASE video_subnet;
GRANT ALL PRIVILEGES ON DATABASE video_subnet TO postgres;
EOF
```

### Set Environment Variables

1. Create a configuration file:

```bash
cp env.example .env
```

2. Edit the .env file and set the following parameters:

```
# Validator Configuration

# Database configuration (PostgreSQL)
DB_NAME=video_subnet
DB_USER=postgres
DB_PASSWORD=YOUR_POSTGRES_PASSWORD
DB_HOST=127.0.0.1
DB_PORT=5432

# ComfyUI Server Configuration
# If your validator program and comfyui are not on the same server, remember to change it.
# Multiple servers can be configured, separated by commas
COMFYUI_SERVERS=127.0.0.1:8188
```

## Validator Startup Command

```bash
python3 neurons/validator.py \
  --netuid 99 \
  --wallet.name [wallet_name] \
  --wallet.hotkey [hotkey] \
  --subtensor.network [network_name] \
  --logging.trace
```
