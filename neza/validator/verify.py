from imagebind import data
import torch
from imagebind.models import imagebind_model
from imagebind.models.imagebind_model import ModalityType
import torch.nn.functional as F
import bittensor as bt
import os
import tempfile
import uuid
import shutil
import requests
from urllib.parse import urlparse
import time
import threading
import queue
from neza.api.comfy_api import ComfyAPI
from neza.utils.misc import copy_audio_wav
import traceback


class VideoVerifier:
    """
    Video Verifier Class
    Uses ImageBind model to verify video similarity
    """

    # Class variables for model caching
    _model_instance = None
    _device = None
    _verification_queue = queue.Queue()
    _verification_lock = threading.Lock()
    _verification_thread = None
    _stop_thread = False

    def __init__(self, validator):
        """Initialize video verifier"""
        self.model, self.device = self._get_model()
        self.validator = validator
        self.comfy_api = ComfyAPI(validator.validator_config.comfy_servers, True)

        if (
            VideoVerifier._verification_thread is None
            or not VideoVerifier._verification_thread.is_alive()
        ):
            VideoVerifier._stop_thread = False
            VideoVerifier._verification_thread = threading.Thread(
                target=self._process_verification_queue,
                daemon=True,
                name="video_verification_queue",
            )
            VideoVerifier._verification_thread.start()
            bt.logging.info("Started video verification queue processor thread")

    def _process_verification_queue(self):
        """Process verification queue thread function"""
        bt.logging.info("Video verification queue processor started")
        while not VideoVerifier._stop_thread:
            try:
                # Get verification task from queue, with timeout of 5 seconds
                try:
                    task = VideoVerifier._verification_queue.get(timeout=5)
                except queue.Empty:
                    continue

                # Unpack task parameters
                args, kwargs, result_container = task

                # Execute actual verification
                try:
                    with VideoVerifier._verification_lock:
                        bt.logging.info("Processing video verification task")
                        result = self._verify_videos_impl(*args, **kwargs)
                        result_container["result"] = result
                        result_container["error"] = None
                except Exception as e:
                    bt.logging.error(f"Error in verification queue processor: {str(e)}")
                    bt.logging.error(traceback.format_exc())
                    result_container["result"] = (0.0, {"error": str(e)})
                    result_container["error"] = e
                finally:
                    # Mark task as completed
                    result_container["completed"] = True
                    # Notify queue that task is done
                    VideoVerifier._verification_queue.task_done()

            except Exception as e:
                bt.logging.error(f"Error in verification queue processor: {str(e)}")
                bt.logging.error(traceback.format_exc())
                time.sleep(1)  # Avoid error loop too fast

        bt.logging.info("Video verification queue processor stopped")

    @classmethod
    def _get_model(cls):
        """Get ImageBind model instance, using singleton pattern to avoid reloading"""
        if cls._model_instance is None:
            cls._device = "cuda:0" if torch.cuda.is_available() else "cpu"
            bt.logging.info(f"Loading ImageBind model to device: {cls._device}")
            cls._model_instance = imagebind_model.imagebind_huge(pretrained=True)
            cls._model_instance.eval()
            cls._model_instance.to(cls._device)
        return cls._model_instance, cls._device

    async def verify_task(
        self, task_id, complete_workflow, completion_time=None, worker_id=None
    ):
        """
        Complete verification flow: execute ComfyUI workflow, download miner video, compare similarity

        Args:
            task_id: Task ID
            complete_workflow: Complete workflow configuration
            completion_time: Task completion time in seconds
            worker_id: Worker ID for server selection

        Returns:
            tuple: (score, verification result dictionary)
        """
        try:
            bt.logging.info(f"Starting verification for task {task_id}")

            # 1. Execute ComfyUI workflow to generate verification video
            bt.logging.info(f"Using ComfyUI to generate verification video: {task_id}")
            # Check if this is a complete workflow configuration
            is_complete_workflow = False
            if isinstance(complete_workflow, dict):
                # Check if it contains nodes with class_type attribute
                for node_id, node in complete_workflow.items():
                    if isinstance(node, dict) and "class_type" in node:
                        is_complete_workflow = True
                        break

            if is_complete_workflow:
                bt.logging.info(
                    f"Using complete workflow configuration for verification: {task_id}"
                )
            else:
                bt.logging.info(
                    f"Using original workflow parameters for verification: {task_id}"
                )

            # Execute workflow to get filename and validator execution time
            output_filename, validator_execution_time, server_info = (
                self.run_comfy_workflow(complete_workflow, task_id, worker_id)
            )

            if not output_filename:
                bt.logging.error(f"Validator failed to generate video: {task_id}")
                return 0.0, {"error": "Failed to generate verification video"}

            # Convert filename to complete URL path
            # Get ComfyAPI server
            if not server_info:
                bt.logging.error("Unable to get ComfyUI server information")
                return 0.0, {"error": "Failed to get ComfyUI server info"}

            # Build complete URL
            host = server_info["host"]
            port = server_info["port"]
            validator_output_url = (
                f"http://{host}:{port}/view?filename={output_filename}&type=output"
            )

            # Download validator-generated video using video manager
            download_success, validator_video_path = (
                await self.validator.video_manager.download_video(
                    task_id, validator_output_url, "validator"
                )
            )

            if not download_success:
                bt.logging.error(
                    f"Unable to download validator-generated video: {validator_output_url}"
                )
                return 0.0, {"error": "Failed to download validator video"}

            # 2. Get miner video path from video manager
            miner_video_path = self.validator.video_manager.get_video_cache_paths(
                task_id, "miner", True
            )

            # Check if miner video exists
            if (
                not os.path.exists(miner_video_path)
                or os.path.getsize(miner_video_path) == 0
            ):
                bt.logging.error(f"Miner video not found or empty: {miner_video_path}")
                return 0.0, {"error": "Failed to get miner video"}

            # 3. Verify video similarity
            bt.logging.info(f"Comparing video similarity: {task_id}")
            score, metrics = self.verify_videos(
                validator_video_path,
                miner_video_path,
                validator_execution_time,
                completion_time,
            )

            # Build result
            result = {
                **metrics,
            }

            bt.logging.info(
                f"Task {task_id} verification completed, similarity: {score:.4f}"
            )
            return score, result

        except Exception as e:
            bt.logging.error(f"Verification task failed: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return 0.0, {"error": str(e)}

    def run_comfy_workflow(self, complete_workflow, task_id=None, worker_id=None):
        """
        Run ComfyUI workflow

        Args:
            complete_workflow: Complete workflow configuration
            worker_id: Worker ID for server selection
        Returns:
            tuple: (output_filename, execution_time, server_info) where output_filename is the generated file,
                  execution_time is in seconds, and server_info contains host and port information
        """
        try:
            bt.logging.info("Executing ComfyUI workflow")

            # Check if this is a complete workflow configuration
            has_class_type = False
            if isinstance(complete_workflow, dict):
                for node_id, node in complete_workflow.items():
                    if isinstance(node, dict) and "class_type" in node:
                        has_class_type = True
                        break

                if has_class_type:
                    bt.logging.info(
                        "Complete workflow configuration detected, using directly"
                    )
                else:
                    bt.logging.warning(
                        "Workflow might be incomplete - missing class_type attributes"
                    )

            # Log workflow complexity
            if isinstance(complete_workflow, dict):
                bt.logging.info(f"Workflow contains {len(complete_workflow)} nodes")

            # Execute workflow with timeout handling
            start_time = time.time()
            bt.logging.info(
                f"Starting ComfyUI workflow execution at {time.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # Use ComfyAPI class to execute workflow
            success, output_filename, server_info, comfy_execution_time = (
                self.comfy_api.execute_comfy_workflow(
                    complete_workflow, task_id, worker_id
                )
            )

            execution_time = (
                comfy_execution_time
                if comfy_execution_time
                else (time.time() - start_time)
            )
            bt.logging.info(
                f"ComfyUI workflow execution completed in {execution_time:.2f} seconds"
            )

            if not success or not output_filename:
                bt.logging.error("ComfyUI workflow execution failed")

                # Log more detailed error information
                if isinstance(complete_workflow, dict):
                    bt.logging.error(f"Workflow node count: {len(complete_workflow)}")

                    # Check for common workflow issues
                    missing_class_types = []
                    for node_id, node in complete_workflow.items():
                        if isinstance(node, dict):
                            if "class_type" not in node:
                                missing_class_types.append(node_id)
                            elif "inputs" in node and not node.get("inputs"):
                                bt.logging.error(
                                    f"Node {node_id} ({node.get('class_type')}) has empty inputs"
                                )

                    if missing_class_types:
                        bt.logging.error(
                            f"Nodes missing class_type attribute: {missing_class_types}"
                        )

                # Check server status
                if server_info:
                    bt.logging.error(
                        f"Used ComfyUI server: {server_info.get('host')}:{server_info.get('port')}"
                    )

                return None, execution_time, None

            bt.logging.info(
                f"ComfyUI workflow executed successfully, output: {output_filename}"
            )

            # Verify output file exists and is accessible
            if server_info and "host" in server_info and "port" in server_info:
                host = server_info["host"]
                port = server_info["port"]
                comfy_url = f"http://{host}:{port}"

                filename = os.path.basename(output_filename)
                video_url = f"{comfy_url}/view?filename={filename}&type=output"

                # bt.logging.info(f"Verifying output file accessibility: {video_url}")

                # Check if file is accessible
                try:
                    response = requests.head(video_url, timeout=10)
                    if response.status_code == 200:
                        bt.logging.info("Output file is accessible")
                    else:
                        bt.logging.warning(
                            f"Output file might not be accessible: status code {response.status_code}"
                        )
                except Exception as e:
                    bt.logging.warning(
                        f"Could not verify output file accessibility: {str(e)}"
                    )

            return output_filename, execution_time, server_info

        except Exception as e:
            bt.logging.error(f"Error executing ComfyUI workflow: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return None, 0, None

    def verify_videos(
        self,
        validator_video_path,
        miner_video_path,
        validator_execution_time=None,
        completion_time=None,
    ):
        """
        Process video verification requests through queue, ensuring only one verification task is executed at a time

        Args:
            validator_video_path: Path to validator video
            miner_video_path: Path to miner video
            validator_execution_time: Validator's execution time in seconds
            completion_time: Task completion time in seconds

        Returns:
            tuple: (similarity score, metrics dictionary)
        """
        # Create result container
        result_container = {"completed": False, "result": None, "error": None}

        # Add task to queue
        bt.logging.info("Adding video verification task to queue")
        args = (
            validator_video_path,
            miner_video_path,
            validator_execution_time,
            completion_time,
        )
        VideoVerifier._verification_queue.put((args, {}, result_container))

        # Wait for task to complete
        start_time = time.time()
        timeout = 600  # 10 minutes timeout
        while not result_container["completed"] and time.time() - start_time < timeout:
            time.sleep(0.5)

        # Check if timeout
        if not result_container["completed"]:
            bt.logging.error(f"Video verification timed out after {timeout} seconds")
            return 0.0, {"error": "Verification timed out"}

        # Check if there is an error
        if result_container["error"]:
            bt.logging.error(
                f"Video verification failed: {str(result_container['error'])}"
            )
            return 0.0, {"error": str(result_container["error"])}

        # Return result
        return result_container["result"]

    def _verify_videos_impl(
        self,
        validator_video_path,
        miner_video_path,
        validator_execution_time=None,
        completion_time=None,
    ):
        """
            Internal method to actually execute video verification, called by queue processor

        Args:
            validator_video_path: Path to validator video
            miner_video_path: Path to miner video
            validator_execution_time: Validator's execution time in seconds
            completion_time: Task completion time in seconds

        Returns:
            tuple: (similarity score, metrics dictionary)
        """
        try:
            bt.logging.info("Calculating video and audio similarity using ImageBind")

            # Generate audio paths from video paths
            validator_audio_path = validator_video_path.replace(".mp4", ".wav")
            miner_audio_path = miner_video_path.replace(".mp4", ".wav")

            copy_audio_wav(validator_video_path, validator_audio_path)
            copy_audio_wav(miner_video_path, miner_audio_path)

            # Set clip parameters
            clip_duration = 2
            clips_per_video = 10

            # Load videos and audio
            inputs = {
                ModalityType.VISION: data.load_and_transform_video_data(
                    [validator_video_path, miner_video_path],
                    self.device,
                    clip_duration=clip_duration,
                    clips_per_video=clips_per_video,
                ),
                ModalityType.AUDIO: data.load_and_transform_audio_data(
                    [validator_audio_path, miner_audio_path],
                    self.device,
                    clip_duration=clip_duration,
                    clips_per_video=clips_per_video,
                ),
            }

            with torch.no_grad():
                embeddings = self.model(inputs)

            # Get video embeddings
            validator_video_embedding = embeddings[ModalityType.VISION][0:1]
            miner_video_embedding = embeddings[ModalityType.VISION][1:2]

            # Calculate video cosine similarity
            video_cos_sim = F.cosine_similarity(
                validator_video_embedding, miner_video_embedding, dim=1
            ).item()

            # Get audio embeddings
            validator_audio_embedding = embeddings[ModalityType.AUDIO][0:1]
            miner_audio_embedding = embeddings[ModalityType.AUDIO][1:2]

            # Calculate audio cosine similarity
            audio_cos_sim = F.cosine_similarity(
                validator_audio_embedding, miner_audio_embedding, dim=1
            ).item()

            # Calculate L2 distances
            video_l2_distance = torch.norm(
                validator_video_embedding - miner_video_embedding, p=2, dim=1
            ).item()

            audio_l2_distance = torch.norm(
                validator_audio_embedding - miner_audio_embedding, p=2, dim=1
            ).item()

            # Calculate metrics
            metrics = {
                "video_cosine_similarity": video_cos_sim,
                "video_l2_distance": video_l2_distance,
                "audio_cosine_similarity": audio_cos_sim,
                "audio_l2_distance": audio_l2_distance,
            }

            # Set weights for score calculation
            video_score_weight = 0.4
            audio_score_weight = 0.2
            runtime_weight = 0.4

            # Process completion time if provided
            runtime_score = 0
            if completion_time is not None:
                # Use validator's execution time as base runtime if available, otherwise use default
                # Ensure base_runtime is at least 10 second to avoid division by zero issues
                base_runtime = (
                    validator_execution_time
                    if validator_execution_time is not None
                    else 10
                )
                base_runtime = max(10, base_runtime)

                metrics["validator_execution_time"] = base_runtime

                # Calculate runtime score component
                runtime_upper_limit = min(base_runtime * 2, completion_time)
                diff_runtime = base_runtime - runtime_upper_limit
                runtime_scale = max(-1, min(1, diff_runtime / 100))
                runtime_score = runtime_scale

                metrics["completion_time"] = completion_time
                metrics["runtime_score"] = runtime_scale

            if video_cos_sim < 0.95 or audio_cos_sim < 0.95:
                bt.logging.warning(
                    f"Low similarity detected: video={video_cos_sim:.4f}, audio={audio_cos_sim:.4f}, setting score to 0"
                )
                metrics["video_component_score"] = video_score_weight * video_cos_sim
                metrics["audio_component_score"] = audio_score_weight * audio_cos_sim
                metrics["runtime_component_score"] = runtime_weight * runtime_score
                metrics["final_score"] = 0
                return 0.0, metrics

            # Calculate combined score
            score = (
                video_score_weight * video_cos_sim
                + audio_score_weight * audio_cos_sim
                + runtime_weight * runtime_score
            )

            # Normalize score to [0, 1] range
            score = max(0, min(1, score))

            # Add component scores to metrics
            metrics["video_component_score"] = video_score_weight * video_cos_sim
            metrics["audio_component_score"] = audio_score_weight * audio_cos_sim
            metrics["runtime_component_score"] = runtime_weight * runtime_score
            metrics["final_score"] = score

            bt.logging.info(
                f"Similarity metrics: video={video_cos_sim:.4f}, audio={audio_cos_sim:.4f}, "
                f"runtime={runtime_score:.4f}, final_score={score:.4f}"
            )
            return score, metrics

        except Exception as e:
            bt.logging.error(f"Error verifying videos: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return 0.0, {"error": str(e)}

    def verify_text_video(self, text, video_path):
        """
        Verify similarity between text and video using ImageBind model

        Args:
            text: Text prompt
            video_path: Path to video

        Returns:
            tuple: (similarity score, metrics dictionary)
        """
        try:
            bt.logging.info(f"Calculating text-video similarity for: {text}")

            # Load inputs
            inputs = {
                ModalityType.TEXT: data.load_and_transform_text([text], self.device),
                ModalityType.VISION: data.load_and_transform_video_data(
                    [video_path], self.device
                ),
            }

            with torch.no_grad():
                embeddings = self.model(inputs)

            # Get embeddings
            text_embedding = embeddings[ModalityType.TEXT]
            video_embedding = embeddings[ModalityType.VISION]

            # Calculate cosine similarity
            text_embedding_norm = F.normalize(text_embedding, p=2, dim=1)
            video_embedding_norm = F.normalize(video_embedding, p=2, dim=1)
            cosine_similarity = torch.sum(
                text_embedding_norm * video_embedding_norm, dim=1
            ).item()

            # Calculate L2 distance
            l2_distance = torch.norm(
                text_embedding - video_embedding, p=2, dim=1
            ).item()

            # Calculate metrics
            metrics = {
                "text_video_cosine_similarity": cosine_similarity,
                "text_video_l2_distance": l2_distance,
            }

            # Normalize score to [0, 1] range
            score = max(0, min(1, cosine_similarity))

            bt.logging.info(
                f"Text-video similarity metrics: cosine={cosine_similarity:.4f}, l2={l2_distance:.4f}, score={score:.4f}"
            )
            return score, metrics

        except Exception as e:
            bt.logging.error(f"Error verifying text-video similarity: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return 0.0, {"error": str(e)}

    def __del__(self):
        """Clean up resources"""
        # Stop verification queue processing thread
        if (
            VideoVerifier._verification_thread
            and VideoVerifier._verification_thread.is_alive()
        ):
            VideoVerifier._stop_thread = True
            # Don't wait for thread to finish, let it finish naturally
