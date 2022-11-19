"""\
Setup Kubernetes on cloud
"""

import logging
import os
import sys

sys.path.append(os.path.abspath("../.."))

import main


def start(config, machines):
    """Setup Kubernetes on cloud VMs using Ansible.

    Args:
        config (dict): Parsed configuration
        machines (list(Machine object)): List of machine objects representing physical machines
    """
    logging.info("Start Kubernetes cluster on VMs")
    processes = []

    # Setup cloud controller
    command = [
        "ansible-playbook",
        "-i",
        os.path.join(config["infrastructure"]["base_path"], ".continuum/inventory_vms"),
        os.path.join(config["infrastructure"]["base_path"], ".continuum/cloud/control_install.yml"),
    ]
    processes.append(machines[0].process(config, command, output=False))

    # Setup cloud worker
    command = [
        "ansible-playbook",
        "-i",
        os.path.join(config["infrastructure"]["base_path"], ".continuum/inventory_vms"),
        os.path.join(config["infrastructure"]["base_path"], ".continuum/cloud/install.yml"),
    ]
    processes.append(machines[0].process(config, command, output=False))

    # Check playbooks
    for process in processes:
        logging.debug("Check output for Ansible command [%s]" % (" ".join(process.args)))
        output = [line.decode("utf-8") for line in process.stdout.readlines()]
        error = [line.decode("utf-8") for line in process.stderr.readlines()]
        main.ansible_check_output((output, error))
