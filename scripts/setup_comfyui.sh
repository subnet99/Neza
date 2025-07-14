#!/bin/bash
set -e

# =============================================================================
# Configuration
# =============================================================================

# Model download URLs
HF_COMFY_ORG="https://huggingface.co/Comfy-Org/Wan_2.1_ComfyUI_repackaged/resolve/main/split_files"
HF_KIJAI="https://huggingface.co/Kijai/WanVideo_comfy/resolve/main"

# ComfyUI models
COMFY_MODELS=(
    "clip_vision/clip_vision_h.safetensors"
    "diffusion_models/wan2.1_i2v_720p_14B_fp8_e4m3fn.safetensors"
    "vae/wan_2.1_vae.safetensors"
)

# Kijai models (path|filename)
KIJAI_MODELS=(
    "models/diffusion_models/fantasytalking_fp16.safetensors|fantasytalking_fp16.safetensors"
    "models/text_encoders/umt5-xxl-enc-bf16.safetensors|umt5-xxl-enc-bf16.safetensors"
)

# Custom nodes repositories
CUSTOM_NODES=(
    "kijai/ComfyUI-KJNodes"
    "Kosinkadink/ComfyUI-VideoHelperSuite"
    "kijai/ComfyUI-WanVideoWrapper"
    "subnet99/ComfyUI-URLLoader"
)

# =============================================================================
# System Requirements Check
# =============================================================================

# Check required tools
check_requirements() {
    echo "Checking system requirements..."
    
    # Check git
    if ! command -v git >/dev/null 2>&1; then
        echo "✗ Error: git is not installed"
        echo "Install with: sudo apt-get install git"
        return 1
    fi
    
    # Check curl
    if ! command -v curl >/dev/null 2>&1; then
        echo "✗ Error: curl is not installed"
        echo "Install with: sudo apt-get install curl"
        return 1
    fi
    
    # Check python3
    if ! command -v python3 >/dev/null 2>&1; then
        echo "✗ Error: python3 is not installed"
        echo "Install with: sudo apt-get install python3 python3-venv"
        return 1
    fi
    
    # Check pm2
    if ! command -v pm2 >/dev/null 2>&1; then
        echo "✗ Error: pm2 is not installed"
        echo "Install with: ./scripts/install_pm2.sh"
        return 1
    fi
    
    echo "✓ All required tools are available"
}

# =============================================================================
# User Configuration
# =============================================================================

# Get user configuration
get_user_config() {
    echo "Enter GPU ID (0-7, default: 0):"
    read -r GPU_ID
    echo "Enter port number (1024-65535, default: 8188):"
    read -r PORT_NUMBER
    
    # Set defaults
    [ -z "$GPU_ID" ] && GPU_ID=0
    [ -z "$PORT_NUMBER" ] && PORT_NUMBER=8188
    
    # Create instance name
    INSTANCE_NAME="comfy${GPU_ID}-${PORT_NUMBER}"
    
    # Check port availability
    PORT_IN_USE=false
    if command -v lsof >/dev/null 2>&1; then
        if lsof -Pi :$PORT_NUMBER -sTCP:LISTEN -t >/dev/null 2>&1; then
            PORT_IN_USE=true
        fi
    elif command -v ss >/dev/null 2>&1; then
        if ss -tuln | grep -q ":$PORT_NUMBER "; then
            PORT_IN_USE=true
        fi
    elif command -v netstat >/dev/null 2>&1; then
        if netstat -tuln | grep -q ":$PORT_NUMBER "; then
            PORT_IN_USE=true
        fi
    fi
    
    if [ "$PORT_IN_USE" = true ]; then
        echo "✗ Error: Port $PORT_NUMBER is already in use"
        return 1
    fi
    
    echo "✓ Configuration: GPU $GPU_ID, Port $PORT_NUMBER"
}

# =============================================================================
# Installation
# =============================================================================

# Setup ComfyUI repository
setup_comfyui() {
    if [ -d "ComfyUI" ]; then
        echo "Updating existing ComfyUI..."
        cd ComfyUI/
        git pull origin master
    else
        echo "Cloning ComfyUI repository..."
        git clone https://github.com/comfyanonymous/ComfyUI
        cd ComfyUI/
    fi

    # Setup Python environment
    if [ ! -d ".venv" ]; then
        echo "Creating Python virtual environment..."
        python3 -m venv .venv
    fi

    echo "Installing Python dependencies..."
    source .venv/bin/activate
    pip install --upgrade pip -q
    pip install -r requirements.txt
}

# Download models
download_models() {
    # Download ComfyUI models
    echo "Downloading ComfyUI models..."
    for model in "${COMFY_MODELS[@]}"; do
        mkdir -p "models/$(dirname "$model")"
        if [ ! -f "models/$model" ]; then
            echo "Downloading $model..."
            curl -L -o "models/$model" "$HF_COMFY_ORG/$model" &
        else
            echo "Skipping $model (exists)"
        fi
    done

    # Download Kijai models
    echo "Downloading Kijai models..."
    for model in "${KIJAI_MODELS[@]}"; do
        path=$(echo "$model" | cut -d'|' -f1)
        file=$(echo "$model" | cut -d'|' -f2)
        mkdir -p "$(dirname "$path")"
        if [ ! -f "$path" ]; then
            echo "Downloading $file..."
            curl -L -o "$path" "$HF_KIJAI/$file" &
        else
            echo "Skipping $file (exists)"
        fi
    done
    wait
}

# Install custom nodes
install_custom_nodes() {
    echo "Installing custom nodes..."
    for repo in "${CUSTOM_NODES[@]}"; do
        node_name=$(basename $repo)
        if [ -d "custom_nodes/$node_name" ]; then
            echo "Updating $node_name..."
            cd "custom_nodes/$node_name"
            git pull origin main
            cd ../..
        else
            echo "Cloning $node_name..."
            git clone https://github.com/$repo custom_nodes/$node_name &
        fi
    done
    wait

    echo "Installing custom node dependencies..."
    for req in custom_nodes/*/requirements.txt; do
        [ -f "$req" ] && pip install -r "$req" &
    done
    wait
}

# =============================================================================
# PM2 Instance Management
# =============================================================================

# Start PM2 instance
start_comfyui_instance() {
    echo "Starting ComfyUI on GPU $GPU_ID with port $PORT_NUMBER..."
    
    # Start instance
    COMFYUI_PATH=$(pwd)
    if CUDA_VISIBLE_DEVICES=$GPU_ID pm2 start "$COMFYUI_PATH/.venv/bin/python" \
        --name "$INSTANCE_NAME" \
        --cwd "$COMFYUI_PATH" \
        -- main.py --listen 0.0.0.0 --port $PORT_NUMBER; then
        echo "✓ Instance started successfully!"
        echo "🌐 GUI available at: http://127.0.0.1:$PORT_NUMBER"
    else
        echo "✗ Failed to start instance. Check logs:"
        pm2 logs "$INSTANCE_NAME" --lines 50
        return 1
    fi
}

# =============================================================================
# Main Execution
# =============================================================================

echo "Checking system requirements..."
check_requirements

echo "Getting user configuration..."
get_user_config

echo "Starting installation process..."
setup_comfyui
download_models
install_custom_nodes

echo "Installation completed!"
echo "Starting PM2 instance..."
start_comfyui_instance
