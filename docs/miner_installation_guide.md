# Miner Installation Guide

## Table of Contents

1. [Environment Requirements](#environment-requirements)
2. [Installation Steps](#installation-steps)
   - [Pre-installation Setup](#pre-installation-setup)
   - [Required on Single Machine](#required-on-single-machine)
3. [Set Environment Variables](#set-environment-variables)
4. [Miner Startup Command](#miner-startup-command)

## Environment Requirements

Before starting the installation, please ensure your system meets the following requirements:

### Miner Node

- Python 3.10 or higher
- NVIDIA driver version > 525
- Meet the [hardware requirements](../README.md#miner-recommended-configuration)

## Installation Steps

### Pre-installation Setup

The following components can be installed on separate machines and can be completed before the main miner setup:

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

### Required on Single Machine

The following components must be installed on the same machine as the miner (cannot be distributed):

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
# ComfyUI Server Configuration
# Configure multiple ComfyUI servers if using distributed GPU setup
# Format: IP1:PORT1,IP2:PORT2,IP3:PORT3
COMFYUI_SERVERS=127.0.0.1:8188
```

## Miner Startup Command

Mainnet

```bash
pm2 start neurons/miner.py -- \
  --netuid 99 \
  --subtensor.network finney \
  --wallet.name [wallet_name] \
  --wallet.hotkey [hotkey] \
  --axon.port {port} \
  --axon.external_ip {ip} \
  --logging.trace
```

Testnet

```bash
pm2 start neurons/miner.py -- \
  --netuid 377 \
  --subtensor.network test \
  --wallet.name [wallet_name] \
  --wallet.hotkey [hotkey] \
  --axon.port {port} \
  --axon.external_ip {ip} \
  --logging.trace
```
