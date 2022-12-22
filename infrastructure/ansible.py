"""\
Generate Ansible inventory files
"""

import sys
import logging
import socket
import string
import os


def create_inventory_machine(config, machines):
    """Create ansible inventory for creating VMs, so ssh to all physical machines is needed

    Args:
        config (dict): Parsed configuration
        machines (list(Machine object)): List of machine objects representing physical machines
    """
    logging.info("Generate Ansible inventory file for physical machines")
    with open(".tmp/inventory", "w", encoding="utf-8") as f:
        # Shared variables between all groups
        f.write("[all:vars]\n")
        f.write("ansible_python_interpreter=/usr/bin/python3\n")
        f.write("ansible_ssh_common_args='-o StrictHostKeyChecking=no'\n")
        f.write("base_path=%s\n" % (config["infrastructure"]["base_path"]))
        f.write("username=%s\n" % (config["username"]))

        # All hosts group
        f.write("\n[all_hosts]\n")

        for machine in machines:
            base = ""
            if config["infrastructure"]["infra_only"]:
                base = "base=%s" % (machine.base_names[0])

            if machine.is_local:
                f.write(
                    "localhost ansible_connection=local username=%s %s\n" % (machine.user, base)
                )
            else:
                f.write(
                    "%s ansible_connection=ssh ansible_host=%s ansible_user=%s username=%s %s\n"
                    % (
                        machine.name_sanitized,
                        machine.ip,
                        machine.user,
                        machine.user,
                        base,
                    )
                )

        # Specific cloud/edge/endpoint groups for installing RM software
        # For machines with cloud VMs
        if config["infrastructure"]["cloud_nodes"]:
            f.write("\n[clouds]\n")
            clouds = 0

            for machine in machines:
                if machine.cloud_controller + machine.clouds == 0:
                    continue

                base = machine.base_names[0]
                if not config["infrastructure"]["infra_only"]:
                    base = [name for name in machine.base_names if "_cloud_" in name][0]

                if machine.is_local:
                    f.write(
                        "localhost ansible_connection=local cloud_controller=%i \
cloud_start=%i cloud_end=%i base_cloud=%s\n"
                        % (
                            machine.cloud_controller,
                            clouds,
                            clouds + machine.clouds - 1,
                            base,
                        )
                    )
                else:
                    f.write(
                        "%s ansible_connection=ssh ansible_host=%s ansible_user=%s \
cloud_controller=%i cloud_start=%i cloud_end=%i base_cloud=%s\n"
                        % (
                            machine.name_sanitized,
                            machine.ip,
                            machine.user,
                            machine.cloud_controller,
                            clouds,
                            clouds + machine.clouds - 1,
                            base,
                        )
                    )

                clouds += machine.clouds

        # For machines with edge VMs
        if config["infrastructure"]["edge_nodes"]:
            f.write("\n[edges]\n")
            edges = 0

            for machine in machines:
                if machine.edges == 0:
                    continue

                base = machine.base_names[0]
                if not config["infrastructure"]["infra_only"]:
                    base = [name for name in machine.base_names if "_edge_" in name][0]

                if machine.is_local:
                    f.write(
                        "localhost ansible_connection=local edge_start=%i \
edge_end=%i base_edge=%s\n"
                        % (edges, edges + machine.edges - 1, base)
                    )
                else:
                    f.write(
                        "%s ansible_connection=ssh ansible_host=%s ansible_user=%s \
edge_start=%i edge_end=%i base_edge=%s\n"
                        % (
                            machine.name_sanitized,
                            machine.ip,
                            machine.user,
                            edges,
                            edges + machine.edges - 1,
                            base,
                        )
                    )

                edges += machine.edges

        # For machines with endpoint VMs
        if config["infrastructure"]["endpoint_nodes"]:
            f.write("\n[endpoints]\n")
            endpoints = 0
            for machine in machines:
                if machine.endpoints == 0:
                    continue

                base = machine.base_names[0]
                if not config["infrastructure"]["infra_only"]:
                    base = [name for name in machine.base_names if "_endpoint" in name][0]

                if machine.is_local:
                    f.write(
                        "localhost ansible_connection=local endpoint_start=%i \
endpoint_end=%i base_endpoint=%s\n"
                        % (endpoints, endpoints + machine.endpoints - 1, base)
                    )
                else:
                    f.write(
                        "%s ansible_connection=ssh ansible_host=%s ansible_user=%s \
endpoint_start=%i endpoint_end=%i base_endpoint=%s\n"
                        % (
                            machine.name_sanitized,
                            machine.ip,
                            machine.user,
                            endpoints,
                            endpoints + machine.endpoints - 1,
                            base,
                        )
                    )

                endpoints += machine.endpoints


def create_inventory_vm(config, machines):
    """Create ansible inventory for setting up Kubernetes and KubeEdge, so ssh to all VMs is needed

    Args:
        config (dict): Parsed configuration
        machines (list(Machine object)): List of machine objects representing physical machines
    """
    logging.info("Generate Ansible inventory file for VMs")

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        host_ip = s.getsockname()[0]
    except socket.gaierror as e:
        logging.error("Could not get host ip with error: %s", e)
        sys.exit()

    with open(".tmp/inventory_vms", "w", encoding="utf-8") as f:
        # TODO: When using Terraform with GCP, host_ip needs to be accessible from the VMs
        #       This is not the case when you use a cluster with a headnode and worker nodes
        #       that get internet access via the headnode.
        f.write("[all:vars]\n")
        f.write("ansible_python_interpreter=/usr/bin/python3\n")
        f.write("ansible_ssh_common_args='-o StrictHostKeyChecking=no'\n")
        f.write("ansible_ssh_private_key_file=%s\n" % (config["ssh_key"]))
        f.write("registry_ip=%s:%i\n" % (host_ip, 5000))
        f.write(
            "continuum_home=%s\n"
            % (os.path.join(config["infrastructure"]["base_path"], ".continuum"))
        )

        # Tier specific groups
        if (config["mode"] == "cloud" or config["mode"] == "edge") and (
            "benchmark" in config and config["benchmark"]["resource_manager"] != "mist"
        ):
            f.write("cloud_ip=%s\n" % (machines[0].cloud_controller_ips[0]))

            # Cloud controller (is always on machine 0)
            f.write("\n[cloudcontroller]\n")
            f.write(
                "%s ansible_connection=ssh ansible_host=%s ansible_user=%s \
username=%s cloud_mode=%i\n"
                % (
                    machines[0].cloud_controller_names[0],
                    machines[0].cloud_controller_ips[0],
                    machines[0].cloud_controller_names[0],
                    machines[0].cloud_controller_names[0],
                    config["mode"] == "cloud",
                )
            )

        # Cloud worker VM group
        if config["mode"] == "cloud":
            f.write("\n[clouds]\n")

            for machine in machines:
                for name, ip in zip(machine.cloud_names, machine.cloud_ips):
                    f.write(
                        "%s ansible_connection=ssh ansible_host=%s \
ansible_user=%s username=%s\n"
                        % (name, ip, name, name)
                    )

        # Edge VM group
        if config["mode"] == "edge":
            f.write("\n[edges]\n")

            for machine in machines:
                for name, ip in zip(machine.edge_names, machine.edge_ips):
                    f.write(
                        "%s ansible_connection=ssh ansible_host=%s \
ansible_user=%s username=%s\n"
                        % (name, ip, name, name)
                    )

        # Endpoint VM group
        if config["infrastructure"]["endpoint_nodes"]:
            f.write("\n[endpoints]\n")
            for machine in machines:
                for name, ip in zip(machine.endpoint_names, machine.endpoint_ips):
                    f.write(
                        "%s ansible_connection=ssh ansible_host=%s \
ansible_user=%s username=%s\n"
                        % (name, ip, name, name)
                    )

        # Only include base VM logic if there are base VMs
        if not machines[0].base_ips:
            return

        # Make group with all base VMs for netperf installation
        f.write("\n[base]\n")
        for machine in machines:
            for name, ip in zip(machine.base_names, machine.base_ips):
                f.write(
                    "%s ansible_connection=ssh ansible_host=%s ansible_user=%s username=%s\n"
                    % (name, ip, name, name)
                )

        # Make specific groups for cloud/edge/endpoint base VM
        if not config["infrastructure"]["infra_only"]:
            if config["mode"] == "cloud" or config["mode"] == "edge":
                f.write("\n[base_cloud]\n")
                for machine in machines:
                    for name, ip in zip(machine.base_names, machine.base_ips):
                        if "base_cloud" in name.rstrip(string.digits):
                            f.write(
                                "%s ansible_connection=ssh ansible_host=%s \
ansible_user=%s username=%s\n"
                                % (name, ip, name, name)
                            )

            if config["mode"] == "edge":
                f.write("\n[base_edge]\n")
                for machine in machines:
                    for name, ip in zip(machine.base_names, machine.base_ips):
                        if "base_edge" in name.rstrip(string.digits):
                            f.write(
                                "%s ansible_connection=ssh ansible_host=%s \
ansible_user=%s username=%s\n"
                                % (name, ip, name, name)
                            )

            if config["infrastructure"]["endpoint_nodes"]:
                f.write("\n[base_endpoint]\n")
                for machine in machines:
                    for name, ip in zip(machine.base_names, machine.base_ips):
                        if "base_endpoint" in name.rstrip(string.digits):
                            f.write(
                                "%s ansible_connection=ssh ansible_host=%s \
ansible_user=%s username=%s\n"
                                % (name, ip, name, name)
                            )


def copy(config, machines):
    """Copy Ansible files to the local machine, base_path directory
    Machines other than the local one don't need Ansible files, Ansible itself will make it work.

    Args:
        config (dict): Parsed configuration
        machines (list(Machine object)): List of machine objects representing physical machines
    """
    logging.info("Start copying Ansible files to all nodes")

    dest = os.path.join(config["infrastructure"]["base_path"], ".continuum/")
    out = []

    # Copy inventory files
    if machines[0].base_ips:
        out.append(
            machines[0].copy_files(config, os.path.join(config["base"], ".tmp/inventory"), dest)
        )

    out.append(
        machines[0].copy_files(config, os.path.join(config["base"], ".tmp/inventory_vms"), dest)
    )

    # Copy the benchmark file if needed
    if (
        not config["infrastructure"]["infra_only"]
        and (config["mode"] == "cloud" or config["mode"] == "edge")
        and config["benchmark"]["resource_manager"] != "mist"
    ):
        path = os.path.join(
            config["base"],
            "application",
            config["benchmark"]["application"],
            "launch_benchmark_%s.yml" % (config["benchmark"]["resource_manager"]),
        )
        d = dest + "launch_benchmark.yml"
        out.append(machines[0].copy_files(config, path, d))

    # Copy playbooks for installing resource managers and execution_models
    if not config["infrastructure"]["infra_only"]:
        if config["mode"] == "cloud" or config["mode"] == "edge":
            # Use Kubeedge setup code for mist computing
            rm = config["benchmark"]["resource_manager"]
            if config["benchmark"]["resource_manager"] == "mist":
                rm = "kubeedge"

            path = os.path.join(config["base"], "resource_manager", rm, "cloud")
            out.append(machines[0].copy_files(config, path, dest, recursive=True))

            if config["mode"] == "edge":
                path = os.path.join(config["base"], "resource_manager", rm, "edge")
                out.append(machines[0].copy_files(config, path, dest, recursive=True))
        if "execution_model" in config:
            path = os.path.join(config["base"], "execution_model")
            out.append(machines[0].copy_files(config, path, dest, recursive=True))

        path = os.path.join(config["base"], "resource_manager/endpoint/endpoint/")
        out.append(machines[0].copy_files(config, path, dest, recursive=True))

    for output, error in out:
        if error:
            logging.error("".join(error))
            sys.exit()
        elif output:
            logging.error("".join(output))
            sys.exit()
