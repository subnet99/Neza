## ComfyUI Download

```sh
git clone https://github.com/comfyanonymous/ComfyUI.git

cd ComfyUI

pip install -r requirements.txt

python main.py
```

## Model safetensors Download

Download links:

- https://huggingface.co/Comfy-Org/Wan_2.1_ComfyUI_repackaged/tree/main/split_files
- https://huggingface.co/Kijai/WanVideo_comfy/tree/main

```sh
/root/ComfyUI/models/
├── clip_vision
│   ├── clip_vision_h.safetensors
│   └── put_clip_vision_models_here
├── diffusion_models
│   ├── fantasytalking_fp16.safetensors
│   ├── put_diffusion_model_files_here
│   └── wan2.1_i2v_720p_14B_fp8_e4m3fn.safetensors
├── text_encoders
│   ├── put_text_encoder_files_here
│   └── umt5-xxl-enc-bf16.safetensors
├── vae
│   ├── put_vae_here
│   └── wan_2.1_vae.safetensors
```

## ComfyUI Plugins Download

Download links:

- https://github.com/kijai/ComfyUI-KJNodes
- https://github.com/subnet99/ComfyUI-URLLoader
- https://github.com/Kosinkadink/ComfyUI-VideoHelperSuite
- https://github.com/kijai/ComfyUI-WanVideoWrapper

```sh
/root/ComfyUI/custom_nodes/
├── ComfyUI-KJNodes
├── ComfyUI-URLLoader
├── ComfyUI-VideoHelperSuite
├── ComfyUI-WanVideoWrapper
└── example_node.py.example
```
