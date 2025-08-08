from imagebind import data
import torch
from imagebind.models import imagebind_model
from imagebind.models.imagebind_model import ModalityType
import torch.nn.functional as F
import bittensor as bt
import os
import requests
from urllib.parse import urlparse
import time
import threading
import queue
from neza.api.comfy_ws_api import ComfyWSAPI
from neza.utils.misc import copy_audio_wav
import traceback
from neza.utils.http import batch_download_outputs


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

        # Create single ComfyWSAPI instance for all servers
        self.comfy_api = ComfyWSAPI(
            validator.validator_config.comfy_servers, clear_queue=True
        )

        bt.logging.info(
            f"Created ComfyWSAPI instance with {len(self.comfy_api.servers)} servers"
        )
        for i, server in enumerate(self.comfy_api.servers):
            bt.logging.info(f"  worker_{i} -> ****:{server['port']}")

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
        """Process verification queue thread function - sequential processing for single model"""
        bt.logging.info("Verification queue processor started (sequential mode)")
        while not VideoVerifier._stop_thread:
            try:
                # Get verification task from queue, with timeout of 5 seconds
                try:
                    task = VideoVerifier._verification_queue.get(timeout=5)
                except queue.Empty:
                    continue

                # Unpack task parameters
                task_type, args = task

                # Process all tasks sequentially with the same lock to ensure only one model operation at a time
                with VideoVerifier._verification_lock:
                    try:
                        if task_type == "video":
                            # Handle video verification
                            (
                                validator_video_path,
                                miner_video_path,
                                validator_execution_time,
                                completion_time,
                                result_container,
                            ) = args

                            bt.logging.info("Processing video verification task")
                            result = self._verify_videos_impl(
                                validator_video_path, miner_video_path
                            )
                            result_container["result"] = result
                            result_container["error"] = None

                        elif task_type == "image":
                            # Handle image verification
                            (
                                validator_image_path,
                                miner_image_path,
                                validator_execution_time,
                                completion_time,
                                result_container,
                            ) = args

                            bt.logging.info("Processing image verification task")
                            result = self._verify_images_impl(
                                validator_image_path, miner_image_path
                            )
                            result_container["result"] = result
                            result_container["error"] = None

                        elif task_type == "audio":
                            # Handle audio verification
                            (
                                validator_audio_path,
                                miner_audio_path,
                                validator_execution_time,
                                completion_time,
                                result_container,
                            ) = args

                            bt.logging.info("Processing audio verification task")
                            result = self._verify_audio_impl(
                                validator_audio_path, miner_audio_path
                            )
                            result_container["result"] = result
                            result_container["error"] = None

                        else:
                            bt.logging.error(
                                f"Unknown verification task type: {task_type}"
                            )
                            result_container = args[-1] if args else None
                            if result_container:
                                result_container["result"] = (
                                    0.0,
                                    {"error": f"Unknown task type: {task_type}"},
                                )
                                result_container["error"] = Exception(
                                    f"Unknown task type: {task_type}"
                                )

                    except Exception as e:
                        bt.logging.error(f"Error in {task_type} verification: {str(e)}")
                        bt.logging.error(traceback.format_exc())
                        result_container = args[-1] if args else None
                        if result_container:
                            result_container["result"] = (0.0, {"error": str(e)})
                            result_container["error"] = e
                    finally:
                        # Mark task as completed
                        result_container = args[-1] if args else None
                        if result_container:
                            result_container["completed"] = True
                        VideoVerifier._verification_queue.task_done()

            except Exception as e:
                bt.logging.error(f"Error in verification queue processor: {str(e)}")
                bt.logging.error(traceback.format_exc())
                time.sleep(1)  # Avoid error loop too fast

        bt.logging.info("Verification queue processor stopped")

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

    async def verify_task_with_package(
        self,
        task_id,
        complete_workflow,
        completion_time=None,
        worker_id=None,
    ):
        """
        Complete verification flow with package support: execute ComfyUI workflow, download miner package, compare similarity

        Args:
            task_id: Task ID
            complete_workflow: Complete workflow configuration
            completion_time: Task completion time in seconds
            worker_id: Worker ID for server selection

        Returns:
            tuple: (score, verification result dictionary)
        """
        try:
            bt.logging.info(f"Starting package verification for task {task_id}")

            # 1. Execute ComfyUI workflow to generate verification package
            bt.logging.info(
                f"Using ComfyUI to generate verification package: {task_id}"
            )

            # Execute workflow to get output info and validator execution time
            output_info, validator_execution_time, server_info = (
                self.run_comfy_workflow(complete_workflow, task_id, worker_id)
            )

            if not output_info:
                bt.logging.error(f"Validator failed to generate video: {task_id}")
                # clear comfy queue
                return 0.0, {"error": "Failed to generate verification video"}

            # 3. Get validator package (from ComfyUI output)
            if not server_info:
                bt.logging.error("Unable to get ComfyUI server information")
                return 0.0, {"error": "Failed to get ComfyUI server info"}

            out_dir = self.validator.video_manager.get_video_cache_paths(
                task_id, "validator", False
            )

            downloaded_files = batch_download_outputs(
                output_info,
                f"http://{server_info['host']}:{server_info['port']}",
                out_dir,
            )

            if not downloaded_files:
                bt.logging.error(
                    f"Failed to download/extract validator package: {task_id}"
                )
                return 0.0, {"error": "Failed to process validator package"}

            # 4. Pre-verify outputs structure compatibility
            bt.logging.info(f"Pre-verifying outputs structure: {task_id}")
            is_compatible, validator_file_mapping, miner_file_mapping, error_msg = (
                self.pre_verify_outputs(task_id)
            )

            if not is_compatible:
                bt.logging.error(f"outputs structure incompatible: {error_msg}")
                return 0.0, {"error": f"outputs structure incompatible: {error_msg}"}

            # 5. Compare packages using pre-verified file mappings
            bt.logging.info(f"Comparing outputs contents: {task_id}")
            score, metrics = self.verify_packages(
                validator_file_mapping,
                miner_file_mapping,
                validator_execution_time,
                completion_time,
            )

            # Build result
            result = {**metrics}

            bt.logging.info(
                f"Task {task_id} package verification completed, similarity: {score:.4f}"
            )
            return score, result

        except Exception as e:
            bt.logging.error(f"Package verification task failed: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return 0.0, {"error": str(e)}

    def pre_verify_outputs(self, task_id):
        """
        Pre-verify outputs by comparing node structure and file formats between validator and miner

        Args:
            task_id: Task ID for cache path generation

        Returns:
            tuple: (is_compatible, miner_file_mapping, validator_file_mapping, error_message)
                   - is_compatible: Boolean indicating if structures are compatible
                   - validator_file_mapping: Dict mapping node_id -> {output_type -> [file_paths]}
                   - miner_file_mapping: Dict mapping node_id -> {output_type -> [file_paths]}
                   - error_message: Description of incompatibility if any
        """
        try:
            bt.logging.info(f"Pre-verifying outputs for task {task_id}")

            # Get cache paths for both miner and validator packages
            miner_extracted_dir = self.validator.video_manager.get_video_cache_paths(
                task_id, "miner", True
            )

            validator_extracted_dir = (
                self.validator.video_manager.get_video_cache_paths(
                    task_id, "validator", True
                )
            )

            bt.logging.info(f"miner_extracted_dir: {miner_extracted_dir}")
            bt.logging.info(f"validator_extracted_dir: {validator_extracted_dir}")

            # Load outputs.json content
            miner_outputs_info = self.validator.video_manager.load_outputs_json(
                miner_extracted_dir
            )
            validator_outputs_info = self.validator.video_manager.load_outputs_json(
                validator_extracted_dir
            )

            if miner_outputs_info is None:
                return (
                    False,
                    validator_outputs_info,
                    miner_outputs_info,
                    "Failed to load miner outputs.json",
                )

            if validator_outputs_info is None:
                return (
                    False,
                    validator_outputs_info,
                    miner_outputs_info,
                    "Failed to load validator outputs.json",
                )

            # Generate file mappings for comparison
            miner_file_mapping = self.validator.video_manager.get_files_from_outputs(
                miner_outputs_info, miner_extracted_dir
            )
            validator_file_mapping = (
                self.validator.video_manager.get_files_from_outputs(
                    validator_outputs_info, validator_extracted_dir
                )
            )

            # compare validator and miner file mapping
            # Check if miner has all validator file keys and same file counts
            validator_keys = set(validator_file_mapping.keys())
            miner_keys = set(miner_file_mapping.keys())

            # Check if miner has all validator nodes
            missing_in_miner = validator_keys - miner_keys
            if missing_in_miner:
                error_msg = f"File mapping mismatch: miner missing validator keys {missing_in_miner}"
                return False, validator_file_mapping, miner_file_mapping, error_msg

            # Check file counts for each validator key
            for key in validator_keys:
                validator_files = validator_file_mapping[key]
                miner_files = miner_file_mapping[key]

                if len(validator_files) != len(miner_files):
                    error_msg = f"File count mismatch for key {key}: "
                    error_msg += f"validator has {len(validator_files)} files, miner has {len(miner_files)} files"
                    return False, validator_file_mapping, miner_file_mapping, error_msg

            bt.logging.info(f"keys: {list(validator_keys)} vs {list(miner_keys)}")

            return True, validator_file_mapping, miner_file_mapping, ""

        except Exception as e:
            error_msg = f"Error in pre-verification: {str(e)}"
            bt.logging.error(error_msg)
            bt.logging.error(traceback.format_exc())
            return False, None, None, error_msg

    def verify_packages(
        self,
        validator_file_mapping,
        miner_file_mapping,
        validator_execution_time=None,
        completion_time=None,
    ):
        """
        Verify similarity between validator and miner packages using pre-verified file mappings

        Args:
            validator_file_mapping: Pre-verified validator file mapping from pre_verify_outputs
            miner_file_mapping: Pre-verified miner file mapping from pre_verify_outputs
            validator_execution_time: Validator execution time
            completion_time: Task completion time

        Returns:
            tuple: (score, metrics dictionary)
        """
        try:
            bt.logging.info("Comparing package contents using pre-verified mappings")

            # Initialize scoring variables
            total_quality_score = 0.0
            total_files = 0
            file_scores = {}

            # Calculate overall speed score once (not per file)
            overall_speed_score = self._calculate_speed_score(
                validator_execution_time, completion_time
            )

            # Compare each key (node_id_output_type)
            for key in set(
                list(validator_file_mapping.keys()) + list(miner_file_mapping.keys())
            ):
                validator_files = validator_file_mapping.get(key, [])
                miner_files = miner_file_mapping.get(key, [])

                # Compare files of this key (should have same count due to pre-verification)
                for i, (validator_file_info, miner_file_info) in enumerate(
                    zip(validator_files, miner_files)
                ):
                    validator_file_path = validator_file_info.get("file_path")
                    miner_file_path = miner_file_info.get("file_path")
                    file_type = validator_file_info.get("file_type", "unknown")
                    file_type_og = validator_file_info.get("file_type_og", "unknown")
                    base_name = validator_file_info.get("base_name", "unknown")

                    if os.path.exists(validator_file_path) and os.path.exists(
                        miner_file_path
                    ):
                        # Calculate quality score based on file type
                        quality_score, metrics = self._calculate_quality_score(
                            validator_file_path,
                            miner_file_path,
                            file_type_og,
                            validator_execution_time,
                            completion_time,
                        )

                        file_key = f"{key}_{i}"
                        file_scores[file_key] = {
                            "quality_score": quality_score,
                            "file_type": file_type,
                            "file_name": base_name,
                            "metrics": metrics,
                        }

                        total_quality_score += quality_score
                        total_files += 1
                    else:
                        bt.logging.warning(
                            f"File not found: validator={validator_file_path}, miner={miner_file_path}"
                        )

            # Calculate final scores
            avg_quality_score = (
                total_quality_score / total_files if total_files > 0 else 0.0
            )

            # Final score: quality * 0.6 + speed * 0.4
            final_score = avg_quality_score * 0.6 + overall_speed_score * 0.4

            # Build metrics
            metrics = {
                "final_score": final_score,
                "quality_score": avg_quality_score,
                "speed_score": overall_speed_score,
                "total_files_compared": total_files,
                "file_scores": file_scores,
                "validator_execution_time": validator_execution_time,
                "completion_time": completion_time,
            }

            bt.logging.info(
                f"Package verification completed, final score: {final_score:.4f}"
            )
            bt.logging.info(
                f"Quality score: {avg_quality_score:.4f}, Speed score: {overall_speed_score:.4f}"
            )
            return final_score, metrics

        except Exception as e:
            bt.logging.error(f"Error verifying packages: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return 0.0, {"error": str(e)}

    def _calculate_quality_score(
        self,
        validator_file_path,
        miner_file_path,
        file_type,
        validator_execution_time=None,
        completion_time=None,
    ):
        """
        Calculate quality score based on file type

        Args:
            validator_file_path: Path to validator file
            miner_file_path: Path to miner file
            file_type: Type of file (video, image, audio, etc.)
            validator_execution_time: Validator execution time
            completion_time: Task completion time

        Returns:
            tuple: (quality_score, metrics dictionary)
        """
        try:
            if file_type == "video":
                # Get video verification result
                video_score, metrics = self.verify_videos(
                    validator_file_path,
                    miner_file_path,
                    validator_execution_time,
                    completion_time,
                )

                quality_score = video_score
                bt.logging.info(
                    f"Video quality score: {quality_score:.4f} video_metrics: {metrics}"
                )

            elif file_type == "image":
                # Image quality score: direct image similarity
                quality_score, metrics = self.verify_images(
                    validator_file_path,
                    miner_file_path,
                    validator_execution_time,
                    completion_time,
                )
                bt.logging.info(
                    f"Image quality score: {quality_score:.4f} image_metrics: {metrics}"
                )

            elif file_type == "audio":
                # Audio quality score: direct audio similarity
                quality_score, metrics = self.verify_audio(
                    validator_file_path,
                    miner_file_path,
                    validator_execution_time,
                    completion_time,
                )
                bt.logging.info(
                    f"Audio quality score: {quality_score:.4f} audio_metrics: {metrics}"
                )

            else:
                # Other file types get fixed score of 0.5
                bt.logging.info(f"Using fixed score 0.5 for file type: {file_type}")
                quality_score = 0.5
                metrics = {}

            return quality_score, metrics

        except Exception as e:
            bt.logging.error(
                f"Error calculating quality score for {file_type}: {str(e)}"
            )
            return 0.0, {}

    def _calculate_speed_score(self, validator_execution_time, completion_time):
        """
        Calculate speed score based on execution times

        Args:
            validator_execution_time: Validator execution time
            completion_time: Task completion time

        Returns:
            float: Speed score between 0 and 1
        """
        try:
            if not validator_execution_time or not completion_time:
                return 0.5  # Default score if times not available

            # Normalize speed score (faster is better)
            # Assuming completion_time includes both validator and miner time
            # We want to reward faster completion relative to validator time
            speed_ratio = validator_execution_time / completion_time

            # Clamp to reasonable range and normalize
            speed_ratio = max(0.1, min(2.0, speed_ratio))
            speed_score = speed_ratio / 2.0  # Normalize to 0-1 range

            return speed_score

        except Exception as e:
            bt.logging.error(f"Error calculating speed score: {str(e)}")
            return 0.5

    def verify_images(
        self,
        validator_image_path,
        miner_image_path,
        validator_execution_time=None,
        completion_time=None,
    ):
        """
        Verify image similarity using ImageBind

        Args:
            validator_image_path: Path to validator image
            miner_image_path: Path to miner image
            validator_execution_time: Validator execution time
            completion_time: Task completion time

        Returns:
            tuple: (similarity score, metrics dictionary)
        """
        try:
            # Create result container
            result_container = {"completed": False, "result": None, "error": None}

            # Add task to queue
            bt.logging.info("Adding image verification task to queue")
            args = (
                validator_image_path,
                miner_image_path,
                validator_execution_time,
                completion_time,
                result_container,
            )
            self._verification_queue.put(("image", args))

            # Wait for result
            max_wait_time = 300  # 5 minutes
            start_time = time.time()
            while not result_container["completed"]:
                if time.time() - start_time > max_wait_time:
                    bt.logging.error("Image verification timeout")
                    return 0.0, {"error": "Image verification timeout"}
                time.sleep(0.1)

            if result_container["error"]:
                bt.logging.error(
                    f"Image verification error: {result_container['error']}"
                )
                return 0.0, {"error": result_container["error"]}

            return result_container["result"]

        except Exception as e:
            bt.logging.error(f"Error in image verification: {str(e)}")
            return 0.0, {"error": str(e)}

    def verify_audio(
        self,
        validator_audio_path,
        miner_audio_path,
        validator_execution_time=None,
        completion_time=None,
    ):
        """
        Verify audio similarity using ImageBind

        Args:
            validator_audio_path: Path to validator audio
            miner_audio_path: Path to miner audio
            validator_execution_time: Validator execution time
            completion_time: Task completion time

        Returns:
            tuple: (similarity score, metrics dictionary)
        """
        try:
            # Create result container
            result_container = {"completed": False, "result": None, "error": None}

            # Add task to queue
            bt.logging.info("Adding audio verification task to queue")
            args = (
                validator_audio_path,
                miner_audio_path,
                validator_execution_time,
                completion_time,
                result_container,
            )
            self._verification_queue.put(("audio", args))

            # Wait for result
            max_wait_time = 300  # 5 minutes
            start_time = time.time()
            while not result_container["completed"]:
                if time.time() - start_time > max_wait_time:
                    bt.logging.error("Audio verification timeout")
                    return 0.0, {"error": "Audio verification timeout"}
                time.sleep(0.1)

            if result_container["error"]:
                bt.logging.error(
                    f"Audio verification error: {result_container['error']}"
                )
                return 0.0, {"error": result_container["error"]}

            return result_container["result"]

        except Exception as e:
            bt.logging.error(f"Error in audio verification: {str(e)}")
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
            # Use ComfyWSAPI to execute workflow on specific server
            success, task_id, server_info = self.comfy_api.execute_workflow_on_server(
                complete_workflow, task_id, worker_id
            )

            if not success:
                bt.logging.error(f"Failed to submit workflow to worker {worker_id}")
                return None, 0, server_info

            # Poll for task completion
            max_poll_time = 3600  # 1 hour max
            poll_interval = 2  # 2 seconds
            start_poll_time = time.time()
            output_info = None

            while time.time() - start_poll_time < max_poll_time:
                task_status = self.comfy_api.get_task_status(task_id, server_info["id"])

                if not task_status:
                    time.sleep(poll_interval)
                    continue

                if task_status.get("status") == "completed":
                    # Get output info from task
                    output_info = task_status.get("output_info")
                    break

                elif task_status.get("status") == "failed":
                    bt.logging.error(f"Task {task_id} failed on worker {worker_id}")
                    break

                time.sleep(poll_interval)
            else:
                bt.logging.error(
                    f"Task {task_id} polling timeout on worker {worker_id}"
                )
                self.comfy_api.stop_task(task_id, server_info)

            execution_time = task_status.get("execution_time", 0)
            self.comfy_api.remove_task(task_id, server_info["id"])

            if not output_info:
                bt.logging.error(
                    f"Task {task_id} polling timeout on worker {worker_id}"
                )
                return None, execution_time, server_info

            bt.logging.info(
                f"ComfyUI workflow execution completed in {execution_time:.2f} seconds"
            )

            return output_info, execution_time, server_info

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
            result_container,
        )
        VideoVerifier._verification_queue.put(("video", args))

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
    ):
        """
            Internal method to actually execute video verification, called by queue processor

        Args:
            validator_video_path: Path to validator video
            miner_video_path: Path to miner video

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
            video_score_weight = 0.8
            audio_score_weight = 0.2

            if video_cos_sim < 0.95 or audio_cos_sim < 0.95:
                bt.logging.warning(
                    f"Low similarity detected: video={video_cos_sim:.4f}, audio={audio_cos_sim:.4f}, setting score to 0"
                )
                metrics["video_component_score"] = video_score_weight * video_cos_sim
                metrics["audio_component_score"] = audio_score_weight * audio_cos_sim
                metrics["final_score"] = 0
                return 0.0, metrics

            # Calculate combined score
            score = (
                video_score_weight * video_cos_sim + audio_score_weight * audio_cos_sim
            )

            # Normalize score to [0, 1] range
            score = max(0, min(1, score))

            # Add component scores to metrics
            metrics["video_component_score"] = video_score_weight * video_cos_sim
            metrics["audio_component_score"] = audio_score_weight * audio_cos_sim
            metrics["final_score"] = score

            bt.logging.info(
                f"Similarity metrics: video={video_cos_sim:.4f}, audio={audio_cos_sim:.4f}, final_score={score:.4f}"
            )
            return score, metrics

        except Exception as e:
            bt.logging.error(f"Error verifying videos: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return 0.0, {"error": str(e)}

    def _verify_images_impl(
        self,
        validator_image_path,
        miner_image_path,
    ):
        """
        Implementation of image similarity verification using ImageBind

        Args:
            validator_image_path: Path to validator image
            miner_image_path: Path to miner image

        Returns:
            tuple: (similarity score, metrics dictionary)
        """
        try:
            bt.logging.info(
                f"Comparing images: {validator_image_path} vs {miner_image_path}"
            )

            # Use inputs dictionary approach for consistency
            inputs = {
                ModalityType.VISION: data.load_and_transform_vision_data(
                    [validator_image_path, miner_image_path],
                    self.device,
                ),
            }

            with torch.no_grad():
                embeddings = self.model(inputs)

            # Get image embeddings
            validator_image_embedding = embeddings[ModalityType.VISION][0:1]
            miner_image_embedding = embeddings[ModalityType.VISION][1:2]

            # Calculate cosine similarity
            similarity = F.cosine_similarity(
                validator_image_embedding, miner_image_embedding, dim=1
            )
            similarity_score = similarity.item()
            image_l2_distance = torch.norm(
                validator_image_embedding - miner_image_embedding, p=2, dim=1
            ).item()

            metrics = {
                "image_cosine_similarity": similarity_score,
                "image_l2_distance": image_l2_distance,
                "final_score": similarity_score,
            }

            bt.logging.info(f"Image similarity score: {similarity_score:.4f}")
            return similarity_score, metrics

        except Exception as e:
            bt.logging.error(f"Error in image verification implementation: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return 0.0, {"error": str(e)}

    def _verify_audio_impl(
        self,
        validator_audio_path,
        miner_audio_path,
    ):
        """
        Implementation of audio similarity verification using ImageBind

        Args:
            validator_audio_path: Path to validator audio
            miner_audio_path: Path to miner audio

        Returns:
            tuple: (similarity score, metrics dictionary)
        """
        try:
            bt.logging.info(
                f"Comparing audio: {validator_audio_path} vs {miner_audio_path}"
            )

            # Use inputs dictionary approach for consistency
            inputs = {
                ModalityType.AUDIO: data.load_and_transform_audio_data(
                    [validator_audio_path, miner_audio_path],
                    self.device,
                ),
            }

            with torch.no_grad():
                embeddings = self.model(inputs)

            # Get audio embeddings
            validator_audio_embedding = embeddings[ModalityType.AUDIO][0:1]
            miner_audio_embedding = embeddings[ModalityType.AUDIO][1:2]

            # Calculate cosine similarity
            similarity = F.cosine_similarity(
                validator_audio_embedding, miner_audio_embedding, dim=1
            )
            similarity_score = similarity.item()
            audio_l2_distance = torch.norm(
                validator_audio_embedding - miner_audio_embedding, p=2, dim=1
            ).item()

            metrics = {
                "audio_cosine_similarity": similarity_score,
                "audio_l2_distance": audio_l2_distance,
                "final_score": similarity_score,
            }

            bt.logging.info(f"Audio similarity score: {similarity_score:.4f}")
            return similarity_score, metrics

        except Exception as e:
            bt.logging.error(f"Error in audio verification implementation: {str(e)}")
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

        # Clean up ComfyWSAPI instance
        try:
            if hasattr(self, "comfy_api"):
                self.comfy_api.shutdown()
                bt.logging.info("Shutdown ComfyWSAPI instance")
        except Exception as e:
            bt.logging.error(f"Error cleaning up ComfyWSAPI instance: {str(e)}")
