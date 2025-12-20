#!/bin/bash
set -e

# =============================================================================
# Configuration
# =============================================================================

# ComfyUI-Manager configuration
COMFYUI_MANAGER_REPO="subnet99/ComfyUI-Manager"
COMFYUI_MANAGER_BRANCH="main"

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
    
    # Check node (required for ecosystem.config.js)
    if ! command -v node >/dev/null 2>&1; then
        echo "✗ Error: node is not installed"
        echo "Install with: curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash - && sudo apt-get install -y nodejs"
        return 1
    fi
    
    echo "✓ All required tools are available"
}

# =============================================================================
# User Configuration
# =============================================================================

# Get user configuration
get_user_config() {
    # Check if arguments provided
    if [ $# -ge 1 ]; then
        PORT_NUMBER=$1
        if [ $# -ge 2 ]; then
            # Shift to get all PM2 process names
            shift
            RESTART_PROCESSES="$*"
        fi
    else
        echo "Enter port number (1024-65535, default: 8188):"
        read -r PORT_NUMBER
    fi
    
    # Set defaults
    [ -z "$PORT_NUMBER" ] && PORT_NUMBER=8188
    
    # Create instance name (Manager doesn't need GPU, just port to identify which ComfyUI instance)
    MANAGER_INSTANCE_NAME="comfyui-manager-${PORT_NUMBER}"
    
    # Get PM2 process names for restart command
    if [ -z "$RESTART_PROCESSES" ]; then
        echo ""
        echo "Enter PM2 process names to restart (space-separated, e.g., comfy0-8188 comfy1-8189):"
        echo "Leave empty if no restart needed:"
        read -r RESTART_PROCESSES
    fi
    
    # Build restart command if processes provided
    if [ -n "$RESTART_PROCESSES" ]; then
        RESTART_COMMAND="pm2 restart $RESTART_PROCESSES"
        echo "✓ Configuration: Port $PORT_NUMBER, Restart command: $RESTART_COMMAND"
    else
        RESTART_COMMAND=""
        echo "✓ Configuration: Port $PORT_NUMBER, No restart command"
    fi
}

# =============================================================================
# Installation
# =============================================================================

# Install ComfyUI-Manager
install_comfyui_manager() {
    echo "Installing ComfyUI-Manager..."
    
    # Check if ComfyUI directory exists
    if [ ! -d "ComfyUI" ]; then
        echo "✗ Error: ComfyUI directory not found"
        echo "Please run setup_comfyui.sh first or ensure ComfyUI is installed"
        return 1
    fi
    
    cd ComfyUI/
    
    # Activate virtual environment
    if [ ! -d ".venv" ]; then
        echo "✗ Error: ComfyUI virtual environment not found"
        echo "Please run setup_comfyui.sh first"
        cd ..
        return 1
    fi
    
    source .venv/bin/activate
    
    # Create custom_nodes directory if it doesn't exist
    mkdir -p custom_nodes
    
    # Clone or update ComfyUI-Manager
    if [ -d "custom_nodes/comfyui-manager" ]; then
        echo "Updating ComfyUI-Manager..."
        cd custom_nodes/comfyui-manager
        
        # Check if there are local changes
        if ! git diff-index --quiet HEAD --; then
            echo "⚠ Found local changes, stashing them before update..."
            git stash push -m "Auto-stash before ComfyUI-Manager update $(date +%Y%m%d_%H%M%S)"
            HAS_STASH=true
        else
            HAS_STASH=false
        fi
        
        git fetch origin
        git checkout $COMFYUI_MANAGER_BRANCH
        
        # Try to pull, if fails due to conflicts, restore stash
        if ! git pull origin $COMFYUI_MANAGER_BRANCH; then
            echo "⚠ Git pull failed, attempting to resolve..."
            if [ "$HAS_STASH" = true ]; then
                echo "Restoring stashed changes..."
                git stash pop || echo "⚠ Warning: Could not restore stashed changes automatically"
            fi
            cd ../..
            return 1
        fi
        
        # Restore stashed changes if any
        if [ "$HAS_STASH" = true ]; then
            echo "Restoring stashed changes..."
            if ! git stash pop; then
                echo "⚠ Warning: Some conflicts occurred when restoring stashed changes"
                echo "You may need to manually resolve conflicts in:"
                git diff --name-only --diff-filter=U
            fi
        fi
        
        cd ../..
    else
        echo "Cloning ComfyUI-Manager..."
        git clone -b $COMFYUI_MANAGER_BRANCH https://github.com/$COMFYUI_MANAGER_REPO custom_nodes/comfyui-manager
    fi
    
    # Install ComfyUI-Manager dependencies
    echo "Installing ComfyUI-Manager dependencies..."
    if [ -f "custom_nodes/comfyui-manager/requirements.txt" ]; then
        pip install -r custom_nodes/comfyui-manager/requirements.txt
    fi
    
    # Install additional dependencies for direct_installer.py
    echo "Installing direct_installer dependencies..."
    pip install aiohttp requests
    
    echo "✓ ComfyUI-Manager installed successfully"
    cd ..
}

# =============================================================================
# PM2 Instance Management
# =============================================================================

# No need to modify ecosystem.config.js - it reads from environment variable

# Start ComfyUI-Manager ecosystem service
start_manager_ecosystem() {
    echo "Starting ComfyUI-Manager ecosystem service..."
    
    # Determine the correct path based on current directory
    if [ -d "ComfyUI" ]; then
        # In project root directory
        COMFYUI_DIR="ComfyUI"
    elif [ -d "custom_nodes/comfyui-manager" ]; then
        # Already in ComfyUI directory
        COMFYUI_DIR="."
    else
        echo "✗ Error: ComfyUI directory not found"
        return 1
    fi
    
    # Check if ecosystem.config.js exists
    if [ -f "$COMFYUI_DIR/custom_nodes/comfyui-manager/ecosystem.config.js" ]; then
        MANAGER_DIR="$COMFYUI_DIR/custom_nodes/comfyui-manager"
    elif [ -f "custom_nodes/comfyui-manager/ecosystem.config.js" ]; then
        MANAGER_DIR="custom_nodes/comfyui-manager"
    else
        echo "✗ Error: ecosystem.config.js not found in ComfyUI-Manager directory"
        return 1
    fi
    
    # Start ecosystem service with environment variable
    cd "$MANAGER_DIR"
    
    # Check if instance already exists
    if pm2 describe "$MANAGER_INSTANCE_NAME" > /dev/null 2>&1; then
        # Instance exists, update environment variable and restart
        echo "Updating existing Manager instance..."
        if [ -n "$RESTART_COMMAND" ]; then
            RESTART_COMMAND="$RESTART_COMMAND" pm2 restart "$MANAGER_INSTANCE_NAME" --update-env
        else
            pm2 restart "$MANAGER_INSTANCE_NAME" --update-env
        fi
    else
        # New instance, start with environment variable
        if [ -n "$RESTART_COMMAND" ]; then
            RESTART_COMMAND="$RESTART_COMMAND" pm2 start ecosystem.config.js --name "$MANAGER_INSTANCE_NAME" --update-env
        else
            pm2 start ecosystem.config.js --name "$MANAGER_INSTANCE_NAME"
        fi
    fi
    
    if [ $? -eq 0 ]; then
        echo "✓ ComfyUI-Manager ecosystem service started successfully!"
        echo "📦 Manager service: $MANAGER_INSTANCE_NAME"
        echo "🔄 Auto-installing custom nodes and models every 120 seconds"
        if [ -n "$RESTART_COMMAND" ]; then
            echo "🔄 Will restart ComfyUI instances: $RESTART_PROCESSES"
        fi
    else
        echo "✗ Failed to start ComfyUI-Manager ecosystem service. Check logs:"
        pm2 logs "$MANAGER_INSTANCE_NAME" --lines 50
        cd - > /dev/null
        return 1
    fi
    
    # Return to original directory
    cd - > /dev/null
}

# =============================================================================
# Status and Management
# =============================================================================

# Show status
show_status() {
    echo ""
    echo "=========================================="
    echo "ComfyUI-Manager Installation Summary"
    echo "=========================================="
    echo "Manager Service: $MANAGER_INSTANCE_NAME"
    echo "Port: $PORT_NUMBER"
    if [ -n "$RESTART_COMMAND" ]; then
        echo "Restart Command: $RESTART_COMMAND"
    else
        echo "Restart Command: Not configured"
    fi
    echo "Manager Status: Active (auto-install enabled)"
    
    echo ""
    echo "PM2 Status:"
    pm2 status
    
    echo ""
    echo "Useful Commands:"
    echo "  pm2 logs $MANAGER_INSTANCE_NAME          # View Manager logs"
    echo "  pm2 restart $MANAGER_INSTANCE_NAME       # Restart Manager"
    echo "  pm2 stop $MANAGER_INSTANCE_NAME          # Stop Manager"
    echo "  pm2 delete $MANAGER_INSTANCE_NAME       # Remove Manager instance"
}

# =============================================================================
# Main Execution
# =============================================================================

echo "=========================================="
echo "ComfyUI-Manager Setup Script"
echo "=========================================="
echo "This script will install and start ComfyUI-Manager ecosystem service"
echo ""
echo "Usage:"
echo "  ./scripts/setup_comfyui_manager.sh [PORT] [PM2_PROCESS1] [PM2_PROCESS2] ..."
echo "  Example: ./scripts/setup_comfyui_manager.sh 8188 comfy0-8188 comfy1-8189"
echo ""

echo "Checking system requirements..."
check_requirements

echo ""
echo "Getting user configuration..."
get_user_config "$@"

echo ""
echo "Starting installation process..."
install_comfyui_manager

echo ""
echo "Installation completed!"
echo "Starting Manager service..."

start_manager_ecosystem

show_status

echo ""
echo "🎉 ComfyUI-Manager setup completed successfully!"

