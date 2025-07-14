# Neza Subnet Installation Guide

## Table of Contents

1. [Environment Requirements](#environment-requirements)
2. [Installation Steps](#installation-steps)
   - [Pre-installation Setup](#pre-installation-setup)
   - [Required on Single Machine](#required-on-single-machine)
3. [Set Environment Variables](#set-environment-variables)
4. [Validator Startup Command](#validator-startup-command)

## Environment Requirements

Before starting the installation, please ensure your system meets the following requirements:

### Validator Node

- CUDA 11.8 or higher
- Python 3.10 or higher
- NVIDIA driver version > 525
- Meet the [hardware requirements](../README.md#validator-recommended-configuration)

## Installation Steps

### Pre-installation Setup

The following components can be installed on separate machines and can be completed before the main validator setup:

#### Install ComfyUI (Pre-installation)

ComfyUI can be installed on separate GPU servers and prepared in advance:

- According to your GPU model, refer to [ComfyUI](https://github.com/comfyanonymous/ComfyUI) to prepare the system environment.
- Install pm2 (skip this step if already installed)

```bash
chmod +x ./scripts/install_pm2.sh
./scripts/install_pm2.sh
```

- Setup ComfyUI

If you have multiple GPUs, simply repeat running `setup_comfyui.sh` for each GPU:

```bash
chmod +x ./scripts/setup_comfyui.sh
./scripts/setup_comfyui.sh
```

**Note**: You can run multiple ComfyUI instances on different GPU servers and configure them in the environment variables.

#### Install PostgreSQL Database (Pre-installation)

PostgreSQL can be installed on a separate database server and prepared in advance:

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

**Note**: If PostgreSQL is installed on a separate server, update the `DB_HOST` in your environment variables to point to the database server's IP address.

### Required on Single Machine

The following components must be installed on the same machine as the validator (cannot be distributed):

#### Install FFmpeg (MUST be on the same server as Python validator)

FFmpeg is required for video processing and MUST be installed on the same server where the Python validator will run:

```bash
# Update package lists
sudo apt update

# Install FFmpeg
sudo apt install -y ffmpeg

# Verify installation
ffmpeg -version
```

#### Clone Repository

```bash
git clone https://github.com/subnet99/Neza.git
cd Neza
```

#### Create Virtual Environment

It's recommended to use venv to create an isolated Python environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

#### Install Dependencies

```bash
# Install PyTorch (choose the appropriate command based on your CUDA version)
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

# Install subnet dependencies
pip install -r requirements.txt

pip install -e .
```

## Set Environment Variables

1. Create a configuration file:

```bash
cp env.example .env
```

2. Edit the .env file and set the following parameters:

```
# Validator Configuration

# Database configuration (PostgreSQL)
# If PostgreSQL is on a separate server, change DB_HOST to the server's IP
DB_NAME=video_subnet
DB_USER=postgres
DB_PASSWORD=YOUR_POSTGRES_PASSWORD
DB_HOST=127.0.0.1
DB_PORT=5432

# ComfyUI Server Configuration
# Configure multiple ComfyUI servers if using distributed GPU setup
# Format: IP1:PORT1,IP2:PORT2,IP3:PORT3
COMFYUI_SERVERS=127.0.0.1:8188

# Weights & Biases
WANDB_API_KEY=
```

## Validator Startup Command

Mainnet

```bash
pm2 start neurons/validator.py -- \
  --netuid 99 \
  --subtensor.network finney \
  --wallet.name [wallet_name] \
  --wallet.hotkey [hotkey] \
  --logging.trace
```

Testnet

```bash
pm2 start neurons/validator.py -- \
  --netuid 377 \
  --subtensor.network test \
  --wallet.name [wallet_name] \
  --wallet.hotkey [hotkey] \
  --logging.trace
```
