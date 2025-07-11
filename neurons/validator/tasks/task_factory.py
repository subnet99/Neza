import random
import json
from typing import Dict, Any, Optional, List

import bittensor as bt
import uuid
from neurons.validator.tasks.task import Task
import traceback


class TaskFactory:
    """
    Factory for creating different types of tasks
    """

    def __init__(self, material_manager):
        """
        Initialize task factory

        Args:
            material_manager: Material manager instance
        """
        self.material_manager = material_manager

    def create_synthetic_task(self, timeout_seconds: int = 1200) -> Task:
        """
        Create a synthetic task with random parameters

        Args:
            timeout_seconds: Task timeout in seconds

        Returns:
            Task instance
        """
        # Get workflow mapping
        workflow_mapping = self.material_manager.get_workflow_mapping()
        if not workflow_mapping:
            bt.logging.warning("No workflow mapping available for synthetic task")
            return None

        # Create task parameters
        task_params = self._generate_task_params()

        # Create task
        task = Task(
            workflow_params=task_params,
            timeout_seconds=timeout_seconds,
            is_synthetic=True,
        )

        bt.logging.debug(f"Created synthetic task {task.task_id}")
        return task

    def create_task_from_params(
        self, workflow_params: Dict[str, Any], timeout_seconds: int = 600
    ) -> Task:
        """
        Create a task from provided parameters

        Args:
            workflow_params: Workflow parameters
            timeout_seconds: Task timeout in seconds

        Returns:
            Task instance
        """
        # Create task
        task = Task(
            workflow_params=workflow_params,
            timeout_seconds=timeout_seconds,
            is_synthetic=False,
        )

        bt.logging.debug(f"Created task {task.task_id} from params")
        return task

    def create_task_from_materials(
        self, materials_params: Dict[str, Any] = None
    ) -> Task:
        """
        Create a task from materials

        Args:
            materials_params: Materials parameters (optional)

        Returns:
            Task instance or None if failed
        """
        try:
            # Get available materials - this generates random material parameters
            materials = self.material_manager.generate_random_materials_params()
            if not materials:
                bt.logging.warning("No materials available for task creation")
                return None

            # Create a task ID that will be used consistently
            task_id = f"{uuid.uuid4().hex}"

            # Use material_manager's create_task_params_from_materials method
            task_params = self.material_manager.create_task_params_from_materials(
                materials, task_id
            )
            if not task_params:
                bt.logging.warning("Failed to generate task parameters from materials")
                return None

            # Create task with the generated parameters AND the same task_id
            # Use validator config for timeout
            timeout_seconds = (
                self.material_manager.validator.validator_config.task_timeout_seconds
            )

            task = Task(
                task_id=task_id,  # Pass the same task_id here
                workflow_params=task_params,
                timeout_seconds=timeout_seconds,
                is_synthetic=True,
            )

            return task

        except Exception as e:
            bt.logging.error(f"Error creating task from materials: {str(e)}")
            return None

    def _generate_task_params(self) -> Dict[str, Any]:
        """
        Generate random task parameters

        Returns:
            Dict with task parameters
        """
        # Get workflow mapping
        workflow_mapping = self.material_manager.get_workflow_mapping()

        # Select random workflow
        workflow_key = random.choice(list(workflow_mapping.keys()))
        workflow = workflow_mapping[workflow_key]

        # Generate parameters based on workflow
        params = {
            "workflow_name": workflow_key,
            "prompt": self._generate_random_prompt(),
        }

        # Add workflow-specific parameters
        params.update(workflow.get("default_params", {}))

        return params

    def _generate_random_prompt(self) -> str:
        """
        Generate a random prompt

        Returns:
            Random prompt string
        """
        # List of sample prompts
        prompts = [
            "high quality, ultra-detailed, photorealistic rendering, sharp focus, crisp edges, cinematic lighting, ray tracing, depth of field",
            "masterpiece, best quality, intricate details, beautiful composition, stunning visuals, finely textured, elegant design, richly colored, soft lighting",
            "volumetric lighting, soft shadows, subsurface scattering, ambient occlusion, physically accurate light, realistic reflections, smooth shading, glossy surface",
        ]

        # Select random prompt
        return random.choice(prompts)

    def _generate_task_params_from_materials(
        self, materials: Dict[str, Any], materials_params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Generate task parameters from materials

        Args:
            materials: Available materials
            materials_params: Materials parameters (optional)

        Returns:
            Dict with task parameters
        """
        try:
            # Initialize parameters with defaults
            params = materials_params or {}

            # Get workflow mapping
            workflow_mapping = self.material_manager.get_workflow_mapping()

            # Select workflow
            workflow_key = params.get("workflow_name")
            if not workflow_key:
                # Select random workflow
                workflow_key = random.choice(list(workflow_mapping.keys()))

            workflow = workflow_mapping.get(workflow_key)
            if not workflow:
                bt.logging.warning(f"Workflow {workflow_key} not found in mapping")
                return None

            # Get prompt from materials or parameters
            prompt = params.get("prompt")
            if not prompt:
                # Select random prompt from materials
                prompts = materials.get("prompts", [])
                if prompts:
                    prompt = random.choice(prompts)
                else:
                    prompt = self._generate_random_prompt()

            # Build task parameters
            task_params = {"workflow_name": workflow_key, "prompt": prompt}

            # Add workflow-specific parameters
            task_params.update(workflow.get("default_params", {}))

            # Override with provided parameters
            if materials_params:
                for key, value in materials_params.items():
                    if value is not None:
                        task_params[key] = value

            return task_params

        except Exception as e:
            bt.logging.error(
                f"Error generating task parameters from materials: {str(e)}"
            )
            return None
