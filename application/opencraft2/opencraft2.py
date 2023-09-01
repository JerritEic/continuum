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
        "base": "%s:base" % (source)
    }

def add_options(_config):
    """Add config options for a particular module

    Args:
        config (ConfigParser): ConfigParser object

    Returns:
        list(list()): Options to add
    """
    settings = [["frequency", int, lambda x: x >= 1, True, None]]
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
    elif not config["infrastructure"]["use_gpu_endpoint"]:
        parser.error("ERROR: Application opencraft2 requires use_gpu_endpoint")
    elif config["execution_model"]["model"] == "openfaas":
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
    #if config["benchmark"]["resource_manager"] == "mist":
    #    return start_worker_mist(config, machines)
    #if config["benchmark"]["resource_manager"] == "baremetal":
    #    return start_worker_baremetal(config, machines)

    return start_worker_kube(config, machines)

def start_worker_kube(config, _machines):
    """Set variables needed when launching the app on workers

    Args:
        config (dict): Parsed configuration
        machines (list(Machine object)): List of machine objects representing physical machines

    Returns:
        (dict): Application variables
    """
    if config["mode"] == "cloud":
        worker_apps = (
            config["infrastructure"]["cloud_nodes"] - 1) * config["benchmark"][
                "applications_per_worker"
        ]
    elif config["mode"] == "edge":
        worker_apps = (
            config["infrastructure"]["edge_nodes"] * config["benchmark"][
                "applications_per_worker"]
        )

    app_vars = {
        #"container_port": 1883,
        #"mqtt_logs": True,
        #"endpoint_connected": int(config["infrastructure"]["endpoint_nodes"] / worker_apps),
        #"cpu_threads": max(1, int(config["benchmark"]["application_worker_cpu"])),
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
