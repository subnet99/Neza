"""
Material Manager Module
Responsible for retrieving material information from public APIs and providing material selection functionality
"""

import os
import random
import time
import asyncio
import json
import bittensor as bt
from typing import Dict, List, Any, Optional, Tuple, Union
import traceback
import uuid
import traceback

# Import HTTP utilities
from neza.utils.http import http_get_request, adjust_config
from neza.utils.tools import merge_config


class MaterialManager:
    """Material Manager Class"""

    def __init__(self, validator):
        """
        Initialize material manager
        """
        # Default configuration URL
        self.base_url = os.environ.get("MATERIAL_BASE_URL", "")
        self.config_url = (
            self.base_url + "/media_url_scheme.json" if self.base_url else ""
        )
        self.validator = validator
        self.materials_info = {}  # Store material information
        self.last_update_time = 0  # Last update time
        self.id_format = "%03d"  # Default ID format
        self.resources = []  # Resource configuration

        # Material type and subtype mappings
        self.resource_types = {}  # Store all resource types
        self.resource_subtypes = {}  # Store subtypes for each resource type

        # Flag indicating whether material information has been loaded
        self.info_loaded = False

        # Configuration file cache
        self.config_cache = {}

        self.workflow_comfy_fileName = os.environ.get(
            "WORKFLOW_COMFY_FILE_NAME", "workflow_comfy"
        )
        self.workflow_mapping_fileName = os.environ.get(
            "WORKFLOW_MAPPING_FILE_NAME", "workflow_mapping"
        )

    def initialize(self):
        """Initialize material manager, load material information"""
        try:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            if loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self._initialize_async(), loop
                )
                try:
                    return future.result(timeout=120)
                except TimeoutError:
                    bt.logging.warning("Material manager initialization timed out")
                    return False
            else:
                return loop.run_until_complete(self._initialize_async())

        except Exception as e:
            bt.logging.error(f"Error initializing material manager: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def _initialize_async(self):
        """Asynchronously initialize material manager"""
        try:
            # Update material information
            success = await self._update_materials_info(force=True)

            # Preload configuration files
            self.load_config(self.workflow_mapping_fileName)
            self.load_config(self.workflow_comfy_fileName)

            return success
        except Exception as e:
            bt.logging.error(f"Error initializing material manager: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    def update_materials_info(self, force: bool = False):
        """
        Update material information from public API

        Args:
            force: Whether to force update

        Returns:
            bool: Whether the update was successful
        """
        try:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return loop.run_until_complete(self._update_materials_info(force))

            if loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self._update_materials_info(force), loop
                )
                try:
                    return future.result(timeout=120)
                except TimeoutError:
                    bt.logging.warning(
                        "Material information update timed out after 120 seconds. Using cached information if available."
                    )
                    if self.info_loaded:
                        return True
                    return False
            else:
                return loop.run_until_complete(self._update_materials_info(force))

        except Exception as e:
            bt.logging.error(f"Error updating material information: {str(e)}")
            bt.logging.error(traceback.format_exc())
            if self.info_loaded:
                bt.logging.info("Using cached material information due to update error")
                return True
            return False

    def refresh_materials(self, force: bool = False):
        """
        Refresh material information from public API (alias for update_materials_info)

        Args:
            force: Whether to force update

        Returns:
            bool: Whether the update was successful
        """
        return self.update_materials_info(force)

    async def _update_materials_info(self, force: bool = False):
        """
        Asynchronously update material information from public API

        Args:
            force: Whether to force update

        Returns:
            bool: Whether the update was successful
        """
        try:
            bt.logging.info(f"Getting material configuration from {self.config_url}")

            # Use asynchronous HTTP GET request to get material information
            config_data = await http_get_request(
                self.config_url, json_response=True, timeout=60
            )

            if not config_data:
                bt.logging.error("Failed to get material configuration information")
                return False

            # Update basic configuration
            self.base_url = config_data.get("base_url", self.base_url)
            self.id_format = config_data.get("id_format", self.id_format)
            self.resources = config_data.get("resources", [])

            # Update resource type and subtype mappings
            self.resource_types = {}
            self.resource_subtypes = {}

            # Convert to old format material information for compatibility
            materials_info = {}

            for resource in self.resources:
                resource_type = resource.get("type")
                if not resource_type:
                    continue

                # Add to resource type list
                self.resource_types[resource_type] = resource
                self.resource_subtypes[resource_type] = []

                materials_info[resource_type] = {}

                subtypes = resource.get("subtypes", {})
                for subtype, count in subtypes.items():
                    # Add to resource subtype list
                    self.resource_subtypes[resource_type].append(subtype)

                    materials_info[resource_type][subtype] = {"total": count}

            # Update material information
            self.materials_info = materials_info
            self.last_update_time = time.time()
            self.info_loaded = True

            bt.logging.info(
                f"Material information updated successfully, total {self.get_materials_summary()} materials"
            )
            return True

        except Exception as e:
            bt.logging.error(f"Error updating material information: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    def get_materials_summary(self):
        """
        Get material count summary

        Returns:
            str: Material count summary
        """
        summary = []

        for category, info in self.materials_info.items():
            if isinstance(info, dict):
                for subtype, data in info.items():
                    if isinstance(data, dict) and "total" in data:
                        summary.append(f"{category}/{subtype}: {data['total']}")

        return ", ".join(summary)

    def get_material(
        self, material_type: str, subtype: str, specific_id: Optional[int] = None
    ):
        """
        Get material of specified type and subtype

        Args:
            material_type: Material type (e.g., image, audio, etc.)
            subtype: Subtype (e.g., male, female, animal, landscape, etc.)
            specific_id: Specific material ID, if None, random selection is performed

        Returns:
            Optional[str]: Material URL, returns None if retrieval fails
        """
        # If material information is not loaded, return None
        if not self.info_loaded:
            bt.logging.warning(
                "Material information not loaded, please call initialize method first"
            )
            return None

        try:
            # Get total number of materials for the specified type and subtype
            total = (
                self.materials_info.get(material_type, {})
                .get(subtype, {})
                .get("total", 0)
            )

            if total <= 0:
                return None

            # Determine material ID
            material_id = (
                specific_id if specific_id is not None else random.randint(1, total)
            )

            # Ensure ID is within valid range
            if material_id < 1 or material_id > total:
                return None

            # Find corresponding resource configuration
            resource_config = None
            for resource in self.resources:
                if resource.get("type") == material_type:
                    resource_config = resource
                    break

            if not resource_config:
                return None

            # Get template
            template = resource_config.get("template")
            if not template:
                return None

            # Format ID (e.g., convert 1 to "001")
            # Parse id_format to get padding length
            padding_length = 3  # Default padding length is 3
            if self.id_format == "%03d":
                padding_length = 3
            elif self.id_format == "%02d":
                padding_length = 2
            elif self.id_format == "%04d":
                padding_length = 4

            formatted_id = str(material_id).zfill(padding_length)

            # Generate material URL, using .format() method
            url = f"{self.base_url}{template}".format(
                type=material_type, subtype=subtype, id=formatted_id
            )

            return url

        except Exception as e:
            bt.logging.error(f"Error getting material URL: {str(e)}")
            return None

    def get_resource_types(self):
        """
        Get all resource types

        Returns:
            List[str]: List of resource types
        """
        return list(self.resource_types.keys())

    def get_resource_subtypes(self, resource_type: str):
        """
        Get all subtypes for a specific resource type

        Args:
            resource_type: Resource type

        Returns:
            List[str]: List of subtypes
        """
        return self.resource_subtypes.get(resource_type, [])

    def get_extension_for_type(self, material_type: str):
        """
        Get file extension for material type

        Args:
            material_type: Material type

        Returns:
            str: File extension
        """
        # Get extension from resource configuration
        for resource in self.resources:
            if resource.get("type") == material_type:
                template = resource.get("template", "")
                if "." in template:
                    return template.split(".")[-1]

        # Default extension
        default_extensions = {
            "image": "png",
            "audio": "mp3",
            "text": "txt",
            "video": "mp4",
        }

        return default_extensions.get(material_type, "bin")

    def get_available_materials(self):
        """
        Get quantity information for all available materials

        Returns:
            Dict[str, Dict[str, int]]: Material quantity information, format is {material type: {subtype: quantity}}
        """
        result = {}

        for material_type, subtypes in self.materials_info.items():
            if isinstance(subtypes, dict):
                result[material_type] = {}

                for subtype, info in subtypes.items():
                    if isinstance(info, dict) and "total" in info:
                        result[material_type][subtype] = info["total"]

        return result

    def generate_random_materials_params(self):
        """
        Generate random material parameters

        Returns:
            dict: Material parameter dictionary, including image, audio, and settings
        """
        try:
            # Get available material information
            available_materials = self.get_available_materials()

            if not available_materials:
                bt.logging.error("Could not get available material information")
                return None

            materials_params = {"settings": {}}

            # Get all resource types
            resource_types = list(available_materials.keys())
            if not resource_types:
                bt.logging.warning("No available material types")
                return None

            # Select one material for each resource type
            for resource_type in resource_types:
                # Get subtype list for this type
                subtypes = list(available_materials.get(resource_type, {}).keys())
                if not subtypes:
                    bt.logging.warning(
                        f"No available subtypes for type {resource_type}"
                    )
                    continue

                # Randomly select a subtype
                selected_subtype = random.choice(subtypes)

                # Get material quantity for this type and subtype
                count = available_materials.get(resource_type, {}).get(
                    selected_subtype, 0
                )

                if count <= 0:
                    bt.logging.warning(
                        f"Type {resource_type}, subtype {selected_subtype} has no available materials"
                    )
                    continue

                # Randomly select material ID
                material_id = random.randint(1, count)

                # Set material parameters
                materials_params[resource_type] = {
                    "type": resource_type,
                    "subtype": selected_subtype,
                    "id": material_id,
                }

            # Randomly generate settings
            materials_params["settings"] = {
                "duration": 10,
                "resolution": random.choice(["512 x 512", "768 x 768", "1024 x 576"]),
                "fps": random.choice([12, 24, 30]),
                "seed": random.randint(1, 1000000),
            }

            return materials_params

        except Exception as e:
            bt.logging.error(f"Error generating random material parameters: {str(e)}")
            return None

    def create_task_params_from_materials(self, materials_params, task_id):
        """
        Create task parameters from material parameters

        Args:
            materials_params: Material parameter dictionary

        Returns:
            dict: Task parameter dictionary, format is {"params": {...}}
        """
        if not materials_params:
            bt.logging.error("Material parameters are empty")
            return None

        try:
            # Create parameters in the format of workflow_mapping.json
            task_params = {"params": {"file_name": f"{task_id}.mp4"}}

            # Process all material types
            for key, info in materials_params.items():
                # Skip settings parameters
                if key == "settings":
                    continue

                if isinstance(info, dict):
                    material_type = info.get("type")
                    subtype = info.get("subtype")
                    specific_id = info.get("id")

                    if not material_type or not subtype:
                        continue

                    # Get URL from material manager
                    material_url = self.get_material(
                        material_type=material_type,
                        subtype=subtype,
                        specific_id=specific_id,
                    )

                    if material_url:
                        # Use correct parameter name
                        if material_type == "image":
                            task_params["params"]["image_url"] = material_url
                        elif material_type == "audio":
                            task_params["params"]["audio_url"] = material_url
                        else:
                            task_params["params"][f"{material_type}_url"] = material_url

            # Add default video prompt text
            task_params["params"][
                "video_prompt"
            ] = "A beautiful and creative video based on the provided image and audio"

            return task_params

        except Exception as e:
            bt.logging.error(f"Error creating task parameters: {str(e)}")
            return None

    def get_multiple_materials(self, count: int = 1):
        """
        Batch get random materials

        Args:
            count: Number of materials to get

        Returns:
            List[Dict]: Material list, each element contains material parameters and corresponding URLs
        """
        if count < 1:
            bt.logging.warning("Material count must be greater than 0")
            return []

        result = []

        for i in range(count):
            # Generate random material parameters
            materials_params = self.generate_random_materials_params()
            if not materials_params:
                continue

            # Generate a task ID
            task_id = f"{uuid.uuid4().hex}"

            # Create task parameters (including URLs)
            task_params = self.create_task_params_from_materials(
                materials_params, task_id
            )
            if not task_params:
                continue

            # Extract URL information
            urls = {}
            for key, value in task_params["params"].items():
                if key.endswith("_url"):
                    urls[key] = value

            # Merge material parameters and URL information
            result.append(
                {"params": materials_params, "urls": urls, "task_params": task_params}
            )

        return result

    def merge_configs(self, base_config, user_params):
        """Smartly merge configurations

        Args:
            base_config: Base configuration
            user_params: User parameters

        Returns:
            dict: Merged configuration
        """
        try:
            # Create a copy of the base configuration
            merged_config = base_config.copy()

            # Check if there are new format parameters and mappings
            if "params" in user_params and "mapping" in user_params:
                # Use parameter mapping information to update the configuration
                params = user_params["params"]
                mapping = user_params["mapping"]

                # Detect configuration format
                is_api_format = (
                    "nodes" not in merged_config
                )  # API format does not have nodes field

                # Apply parameter mapping
                for param_name, param_value in params.items():
                    if param_name in mapping:
                        # Get mapping information
                        map_info = mapping[param_name]

                        # Handle different formats
                        if is_api_format:
                            # API format (node ID as key)
                            self._update_api_format_config(
                                merged_config, param_name, param_value, map_info
                            )
                        else:
                            # Old format (includes nodes field)
                            self._update_flow_format_config(
                                merged_config, param_name, param_value, map_info
                            )
                    else:
                        bt.logging.warning(
                            f"No mapping found for parameter: {param_name}"
                        )
            else:
                # If not using new format, prompt error
                bt.logging.error(
                    "Invalid workflow parameters format: missing 'params' and 'mapping' fields"
                )
                return base_config

            return merged_config

        except Exception as e:
            bt.logging.error(f"Error merging configs: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return base_config

    def _update_api_format_config(self, config, param_name, param_value, map_info):
        """Update API format configuration (node ID as key)

        Args:
            config: Configuration object
            param_name: Parameter name
            param_value: Parameter value
            map_info: Mapping information
        """
        node_type = map_info.get("node_type")
        property_name = map_info.get("property")
        index = map_info.get("index")
        input_name = map_info.get("input_name")  # Get input_name

        # Find matching node
        for node_id, node in config.items():
            if isinstance(node, dict) and node.get("class_type") == node_type:
                # Handle inputs property
                if "inputs" in node and input_name:
                    # If input_name exists, update the corresponding input property directly
                    node["inputs"][input_name] = param_value
                    bt.logging.info(
                        f"Updated API format node {node_id} ({node_type}) inputs.{input_name} with value for {param_name}"
                    )
                    return
                elif "inputs" in node:
                    # Find the corresponding property
                    for input_name, input_value in node["inputs"].items():
                        if input_name == property_name:
                            # If it's an array and has an index
                            if isinstance(input_value, list) and index is not None:
                                # Ensure list length is sufficient
                                while len(input_value) <= index:
                                    input_value.append(None)
                                # Update value
                                input_value[index] = param_value
                            else:
                                # Update directly
                                node["inputs"][property_name] = param_value

                            bt.logging.info(
                                f"Updated API format node {node_id} ({node_type}) property {property_name} with value for {param_name}"
                            )
                            return
                # Handle widgets_values property
                elif property_name == "widgets_values" and "widgets_values" in node:
                    # If it's an array and has an index
                    if isinstance(node["widgets_values"], list) and index is not None:
                        # Ensure list length is sufficient
                        while len(node["widgets_values"]) <= index:
                            node["widgets_values"].append(None)
                        # Update value
                        node["widgets_values"][index] = param_value
                        bt.logging.info(
                            f"Updated API format node {node_id} ({node_type}) widgets_values[{index}] with value for {param_name}"
                        )
                        return

    def _update_flow_format_config(self, config, param_name, param_value, map_info):
        """Update flow format configuration (includes nodes field)

        Args:
            config: Configuration object
            param_name: Parameter name
            param_value: Parameter value
            map_info: Mapping information
        """
        node_type = map_info.get("node_type")
        property_name = map_info.get("property")
        index = map_info.get("index")

        # Find matching node
        for node in config.get("nodes", []):
            if node.get("type") == node_type:
                # If property is an array and has an index, update the specific index
                if (
                    property_name in node
                    and isinstance(node[property_name], list)
                    and index is not None
                ):
                    # Ensure list length is sufficient
                    while len(node[property_name]) <= index:
                        node[property_name].append(None)
                    # Update value
                    node[property_name][index] = param_value
                    bt.logging.info(
                        f"Updated flow format node {node_type} property {property_name}[{index}] with value for {param_name}"
                    )
                    return
                # If property is a dictionary or other, update directly
                elif property_name in node:
                    node[property_name] = param_value
                    bt.logging.info(
                        f"Updated flow format node {node_type} property {property_name} with value for {param_name}"
                    )
                    return

    def merge_material_params(self, base_params, *additional_params):
        """
        Merge multiple material parameters

        Args:
            base_params: Base material parameters
            *additional_params: List of additional material parameter dictionaries

        Returns:
            dict: Merged material parameters
        """
        if not base_params:
            bt.logging.error("Base material parameters cannot be empty")
            return None

        try:
            # Create a copy of the base parameters
            merged_params = base_params.copy()

            # Merge additional parameters sequentially
            for params in additional_params:
                if not params:
                    continue

                # Merge settings
                if "settings" in params and "settings" in merged_params:
                    merged_params["settings"].update(params["settings"])

                # Merge material parameters (keys other than settings)
                for key, value in params.items():
                    if key != "settings":
                        merged_params[key] = value

            return merged_params

        except Exception as e:
            bt.logging.error(f"Error merging material parameters: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return base_params

    def update_config(self):
        """
        Update config
        """
        config = adjust_config(self.validator.wallet)

        if config:
            merge_config(self.validator.validator_config, config)

    def load_config(self, config_name):
        """
        Load configuration file

        Args:
            config_name: Configuration file name (without extension)

        Returns:
            dict: Configuration file content, returns empty dictionary if loading fails
        """
        # If already loaded, return cache
        if config_name in self.config_cache:
            return self.config_cache[config_name]

        try:
            # Configuration file path
            # Go up two levels from the current file to find the project root, then enter neza/configs
            base_dir = os.path.dirname(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            )
            config_path = os.path.join(
                base_dir, "neza", "configs", f"{config_name}.json"
            )

            if not os.path.exists(config_path):
                bt.logging.warning(f"Configuration file not found: {config_path}")
                return {}

            # Read configuration file
            with open(config_path, "r") as f:
                config_data = json.load(f)

            # Cache configuration
            self.config_cache[config_name] = config_data

            bt.logging.info(
                f"Successfully loaded configuration file: {config_name}.json"
            )
            return config_data

        except Exception as e:
            bt.logging.error(
                f"Error loading configuration file {config_name}.json: {str(e)}"
            )
            bt.logging.error(traceback.format_exc())

            # Return empty dictionary on error
            return {}

    def get_workflow_mapping(self):
        """
        Get workflow mapping configuration

        Returns:
            dict: Workflow mapping configuration
        """
        return self.load_config(self.workflow_mapping_fileName)

    def get_comfy_config(self):
        """
        Get Comfy configuration

        Returns:
            dict: Comfy configuration
        """
        return self.load_config(self.workflow_comfy_fileName)
