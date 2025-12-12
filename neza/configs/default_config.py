score_manager_default_config = {
    "cache_version": 1,
    "min_cache_version": 1,
    "history_weight": 0.4,
    "current_weight": 0.6,
    "max_history": 6,
    "sliding_window": 30,
    "always_save_cache": True,
    "comfy_task_weight": 0.2,
    "api_task_weight": 0.8,
    # Emission model: segmented exponential
    "emission_min": 1.0,
    "emission_max": 1.0,
    "emission_max_pro": 1.0,
    "emission_k1": 3.0,
    "emission_k2": 0.5,
    "emission_transition": 0.5,
    # Miner stake factor: segmented exponential
    "miner_factor_min": 1.0,
    "miner_factor_max": 1.0,
    "miner_factor_k1": 4.0,
    "miner_factor_k2": 1.0,
    "miner_factor_transition": 0.5,
    # Sora-2 time scoring parameters
    "sora2_baseline_time": 120.0,  # 2 minutes baseline time in seconds
    "sora2_max_reward_time": 60.0,  # Time threshold for maximum reward (1.3)
    "sora2_time_penalty_factor": 1.5,  # Penalty factor for slower than baseline
    "sora2_time_sigmoid_steepness": 3.0,  # Steepness of time penalty curve
    "sora2_time_min_factor": 0.3,  # Minimum time factor (penalty floor)
    "sora2_time_max_factor": 1.3,  # Maximum time factor (reward ceiling)
}
