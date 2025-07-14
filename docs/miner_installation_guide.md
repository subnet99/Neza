# Miner Installation Guide

## Table of Contents

1. [Environment Requirements](#environment-requirements)
2. [Installation Steps](#installation-steps)
   - [Clone Repository](#clone-repository)
   - [Create Virtual Environment](#create-virtual-environment)
   - [Install Dependencies](#install-dependencies)
3. [Install ComfyUI](#install-comfyui)
4. [Set Environment Variables](#set-environment-variables)
5. [Miner Startup Command](#miner-startup-command)

## Environment Requirements

Before starting the installation, please ensure your system meets the following requirements:

### Miner Node

- Python 3.10 or higher
- NVIDIA driver version > 525
- Meet the [hardware requirements](../README.md#miner-recommended-configuration)

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

## Set Environment Variables

1. Create a configuration file:

```bash
cp env.example .env
```

2. Edit the .env file and set the following parameters:

```
# ComfyUI Server Configuration
# If your validator program and comfyui are not on the same server, remember to change it.
# Multiple servers can be configured, separated by commas
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