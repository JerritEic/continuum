"""Manage the opencraft2 application"""

import logging
import copy
import sys
import numpy as np
import pandas as pd

from application import application


def set_container_location(config):
    """Set registry location/path of containerized applications

    Args:
        config (dict): Parsed configuration
    """
    source = "jerriteic/opencraft2"
    # Container applications
    config["images"] = {
        "endpoint": "%s:base" % (source),
        "worker": "%s:headless" % (source)
    }

def add_options(_config):
    """Add config options for a particular module

    Args:
        config (ConfigParser): ConfigParser object

    Returns:
        list(list()): Options to add
    """
    # Option | Type | Condition | Mandatory | Default
    settings = [["opencraft_streamed_client_ratio", float, lambda x: x >= 0.0, False, 0.0],
                ["opencraft_experiment_duration", int, lambda x: x >= 0, True, 60],
                ["opencraft_tps", int, lambda x: x >= 0, False, 60],
                ["opencraft_endpoint_remote_config", bool, lambda _: True, False, True],
                ["opencraft_player_emulation_type", str, lambda _: True, False, "Playback"],
                ["opencraft_player_emulation_recording_file", str, lambda _: True, False, "42_recording5.inputtrace"],
                ["opencraft_player_emulation_simulation_behaviour", str, lambda _: True, False, "BoundedRandom"],
                ["opencraft_terrain_type", str, lambda _: True, False, "default"],
                ["opencraft_num_simulated_players", int, lambda x: x >= 0, False, 0],
                ["opencraft_simulated_player_join_interval", int, lambda x: x >= 0, False, 0],
                # ["opencraft_endpoint_cpu", list, lambda _: True, False, []],
                ["opencraft_deployment_config", str, lambda _: True, True, ""],
                ["opencraft_server_baremetal", bool, lambda _: True, False, False],]
    return settings


def verify_options(parser, config):
    """Verify the config from the module's requirements

    Args:
        parser (ArgumentParser): Argparse object
        config (ConfigParser): ConfigParser object
    """
    if config["benchmark"]["application"] != "opencraft2":
        parser.error("ERROR: Application should be opencraft2")
    elif "cache_worker" in config["benchmark"] and config["benchmark"]["cache_worker"] == "True":
        parser.error("ERROR: opencraft2 app does not support application caching")
    elif config["benchmark"]["resource_manager"] == "kubecontrol":
        parser.error("ERROR: Application opencraft2 does not support kubecontrol")
    elif config["infrastructure"]["endpoint_nodes"] <= 0:
        parser.error("ERROR: Application opencraft2 requires at least 1 endpoint")
    elif "execution_model" in config and config["execution_model"]["model"] == "openfaas":
        parser.error("ERROR: Application opencraft2 does not support OpenFAAS")


def start_worker(config, machines):
    """Set variables needed when launching the app on workers

    Args:
        config (dict): Parsed configuration
        machines (list(Machine object)): List of machine objects representing physical machines

    Returns:
        (dict): Application variables
        OR
        (list): Application variables
    """
    app_vars = {
         "experiment_duration": int(config["benchmark"]["opencraft_experiment_duration"]),
         "deployment_config": str(config["benchmark"]["opencraft_deployment_config"]),
         "terrain_type": str(config["benchmark"]["opencraft_terrain_type"]),
         "tps": int(config["benchmark"]["opencraft_tps"])
    }
    return app_vars



def gather_worker_metrics(_machines, _config, worker_output, _starttime):
    """Gather metrics from cloud or edge workers for the opencraft2 app

    Args:
        machines (list(Machine object)): List of machine objects representing physical machines
        config (dict): Parsed configuration
        worker_output (list(list(str))): Output of each container ran on the edge
        starttime (datetime): Time that 'kubectl apply' is called to launched the benchmark

    Returns:
        list(dict): List of parsed output for each cloud or edge worker
    """
    # TODO
    pass


def gather_endpoint_metrics(config, endpoint_output, container_names):
    """Gather metrics from endpoints

    Args:
        config (dict): Parsed configuration
        endpoint_output (list(list(str))): Output of each endpoint container
        container_names (list(str)): Names of docker containers launched

    Returns:
        list(dict): List of parsed output for each endpoint
    """
    # TODO
    pass


def format_output(config, worker_metrics, endpoint_metrics, status=None):
    """Format processed output to provide useful insights (opencraft2)

    Args:
        config (dict): Parsed configuration
        sub_metrics (list(dict)): Metrics per worker node
        endpoint_metrics (list(dict)): Metrics per endpoint
    """
    # TODO
    pass
