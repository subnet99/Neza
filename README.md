<div align="center">

# **Neza**
Focused on faster, higher-quality video generation, driving innovation, empowering creators, and enabling commercial impact.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[Bittensor](https://bittensor.com/) • [Discord](https://discord.com/channels/799672011265015819/1366845111945658409) • [Neza](https://neza.video)
</div>

# Overview

Neza (Subnet 99) is a fully decentralized network for AI-powered video generation, built on cutting-edge open-source models. Our mission is to create a resilient, peer-to-peer platform that makes high-quality AI video content accessible to everyone.

Neza shards advanced generation models like WAN2.1 across the network. Validators publish workflows, miners execute them to generate videos, and validators then assess output quality and distribute rewards—together forming a scalable, trustworthy ecosystem for on-demand AI video production.

**Note:** By downloading or running this software, you agree to comply with the terms and conditions outlined in our agreement.


## Table of Contents

1. [Overview](#overview)  
2. [Decentralization](#decentralization)  
   - [Decentralized Miners](#decentralized-miners)  
   - [Decentralized Validators](#decentralized-validators)  
3. [Hardware Requirements](#hardware-requirements)  
   - [Miner Recommended Configuration](#miner-recommended-configuration)  
   - [Validator Recommended Configuration](#validator-recommended-configuration)  
4. [Installation](#installation)  
5. [How to Run Neza](#how-to-run-neza)  
   - [Running a Miner](#running-a-miner)  
     - [ComfyUI Interface Configuration](#comfyui-interface-configuration)  
     - [Miner Startup Command](#miner-startup-command)  
   - [Running a Validator](#running-a-validator)  
     - [Validator Startup Command](#validator-startup-command)  
6. [Workflow Mechanism](#workflow-mechanism)  
   - [Synthetic Workflows](#synthetic-workflows)  
   - [Organic Workflows](#organic-workflows)  
7. [Miner Optimization Directions](#miner-optimization-directions)  
   - [Hardware Optimization](#hardware-optimization)  
   - [Concurrent Processing](#concurrent-processing)

## Decentralization

Decentralization is at the heart of Neza: we don’t depend on any central API—instead, each miner and validator runs its own model.  

### Decentralized Miners

Miners run open-source video models through ComfyUI, processing workflows dispatched by validators. They earn rewards based on:

- **Video Quality**: Rated against benchmark models using ImageBind.  
- **Speed**: Measured in seconds per frame.  

Higher quality and faster performance yield greater rewards! 

### Decentralized Validators

Validators act as decentralized gateways: they define workflows, dispatch them to miners, and use ImageBind to assess the quality of each video.

Each validator runs its own transparent scoring system—rewarding miners based on video quality, speed and reliability. There’s no central authority; all validation happens on the validators’ hardware.

## Hardware Requirements

### Miner Recommended Configuration

Miners' GPUs should meet or exceed:

- **GeForce RTX 4090 (24 GB GDDR6X):** only for 480P video generation.
- **NVIDIA A100 40 GB HBM2:** supports 480P and 720P at moderate speed.

System requirements:

- **Storage:** 200 GB SSD.  
- **System RAM:** 32 GB.
- **CPU:** 8 cores.

### Validator Recommended Configuration

Validators's GPUs should meet or exceed:
- **NVIDIA A100 40 GB HBM2**

System requirements:
- **Storage:** 500 GB SSD.  
- **System RAM:** 64 GB.  
- **CPU:** 16 cores.  
- **Network:** ≥ 100 Mbps.

Validators run multiple video validation tasks in parallel and execute the compute-intensive ImageBind model for quality checks, so they require more powerful hardware to keep the network running smoothly.  

## Installation

Please refer to the [Installation Guide](docs/installation.md) for detailed installation steps.

## How to Run Neza

### Running a Miner

Miner nodes run on ComfyUI and must be configured with the appropriate video models to handle generation requests.  

#### ComfyUI Interface Configuration

Miners must set up ComfyUI with the required video models (see [Model List](docs/models.md)).  

Miners use the ComfyAPI (default endpoint: `http://127.0.0.1:8188`) to communicate with ComfyUI. To connect to multiple servers, set the `COMFYUI_SERVERS` environment variable to a comma-separated list of URLs.  

#### Miner Startup Command

```bash
python3 neurons/miner.py \
  --netuid 99 \
  --wallet.name [wallet_name] \
  --wallet.hotkey [hotkey] \
  --subtensor.network [network_name] \
  --logging.trace
```

Miners implement a task queue to handle multiple concurrent workflows and expose task status queries. Upon receiving a workflow from a validator, the miner will:

1. Enqueue the task  
2. Execute the workflow via the ComfyAPI  
3. Upload the generated video to the designated public-cloud storage

### Running a Validator

Validators handle workflow configurations—retrieving or defining them, dispatching them to miners for execution, and validating the quality of the generated videos.

#### Validator Startup Command

```bash
python3 neurons/validator.py \
  --netuid 99 \
  --wallet.name [wallet_name] \
  --wallet.hotkey [hotkey] \
  --subtensor.network [network_name] \
  --logging.trace
```

Validators use the `VideoVerifier` (built on ImageBind) to assess generated videos through the following steps:

1. Retrieve or define workflow configurations  
2. Dispatch workflows to miners  
3. Await video generation results  
4. Evaluate video quality with ImageBind  
5. Score miners on quality and response time  
6. Normalize scores to determine subnet weights  

They also run asynchronous pipelines that dispatch tasks to multiple miners in parallel and compare outputs.

## Workflow Mechanism

Neza supports two types of workflows: synthetic workflows and organic workflows.

### Synthetic Workflows

Synthetic workflows are preset test tasks defined by validators to benchmark miners—measuring video quality, processing speed, and reliability.

### Organic Workflows

Organic workflows come from real user requests. Neza lets Validators push these requests straight into the database so validators can dispatch them to miners in real time—ensuring miners not only run benchmark tests but also fulfill actual needs.


## Miner Optimization Directions

To earn higher rewards in Neza, miners can optimize in the following aspects:

### Hardware Optimization

1. **GPU Upgrade**: Using higher-performance GPUs (such as H200, B200) can significantly improve video generation speed
2. **High-Speed Storage**: Use NVMe SSDs to store models and temporary files, reducing I/O bottlenecks
3. **Memory Optimization**: Increase system memory to 64GB or more, reducing memory swapping and improving stability
4. **Network Bandwidth**: Upgrade to gigabit or higher bandwidth to ensure fast video uploads

### Concurrent Processing

1. **Task Scheduling System**: Implement an efficient task scheduling system that dynamically allocates resources based on task priority and resource availability
2. **Multi-Instance Deployment**: Deploy multiple ComfyUI instances, each connected to different GPUs
3. **Load Balancing**: Implement intelligent load balancing, assigning tasks to the most suitable GPU
4. **Queue Optimization**: Optimize task queue management to reduce waiting time
5. **Warm-up Mechanism**: Keep models in a warm-up state in memory to reduce cold start time