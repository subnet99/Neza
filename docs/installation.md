# Neza Subnet Installation Guide

This guide will help you install and configure Neza subnet (SN99) miner and validator nodes.

## Table of Contents

1. [Environment Requirements](#environment-requirements)
2. [Installation Steps](#installation-steps)
   - [Clone Repository](#clone-repository)
   - [Create Virtual Environment](#create-virtual-environment)
   - [Install Dependencies](#install-dependencies)
3. [Miner Node Configuration](#miner-node-configuration)
   - [Install ComfyUI](#install-comfyui)
   - [Configure Video Generation Models](#configure-video-generation-models)
   - [Configure ComfyUI](#configure-comfyui)
4. [Validator Node Configuration](#validator-node-configuration)
   - [Install Evaluation Models](#install-evaluation-models)
   - [Configure Validation Parameters](#configure-validation-parameters)
5. [Environment Variable Configuration](#environment-variable-configuration)
6. [Common Issues](#common-issues)

## Environment Requirements

Before starting the installation, please ensure your system meets the following requirements:

### Miner Node
- CUDA 11.8 or higher
- Python 3.10 or higher
- NVIDIA driver version > 525
- Meet the [hardware requirements](../README.md#miner-recommended-configuration)

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

It's recommended to use conda to create an isolated Python environment:

```bash
conda create -n neza python=3.10
conda activate neza
```

Or use venv:

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate  # Windows
```

### Install Dependencies

```bash
# Install PyTorch (choose the appropriate command based on your CUDA version)
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

# Install subnet dependencies
pip install -r requirements.txt

pip install -e .
```

## Miner Node Configuration

### Install ComfyUI

Miner nodes need to install [ComfyUI](https://github.com/comfyanonymous/ComfyUI.git) as the backend for video generation:

### Configure Video Generation Models

Miner nodes need to configure high-quality video generation models. Please refer to the [Supported Models](./models.md) document for detailed model information and download links.

You need to place the downloaded model files in the appropriate location within the ComfyUI models directory. After the models are installed, you can verify that they are loaded correctly through the ComfyUI interface.

## Validator Node Configuration

### Configure Validation Parameters

Validator nodes need to configure some parameters to optimize the validation process:

1. Create a configuration file:

```bash
cp env.example .env
```

2. Edit the .env file and set the following parameters:

## Environment Variable Configuration

To properly configure the Neza subnet, you need to set some environment variables:

### Miner Node Environment Variables

Create a `.env` file in the project root directory and add the following content:

```
# ComfyUI Server Configuration
COMFYUI_SERVERS=127.0.0.1:8188  # Multiple servers can be configured, separated by commas
```

### Validator Node Environment Variables

```
# Validator Configuration

# Database configuration (PostgreSQL)
DB_NAME=video_subnet         # Database name
DB_USER=postgres             # Database username
DB_PASSWORD=postgres         # Database password
DB_HOST=localhost            # Database host
DB_PORT=5432                 # Database port

# ComfyUI Server Configuration
COMFYUI_SERVERS=127.0.0.1:8188  # Multiple servers can be configured, separated by commas

```