import json
import os
import time
import subprocess
from typing import Any
import docker
import aerospike

SET_NAME = "aql-tests"
AEROSPIKE_VERSION = "latest"

def run_containers(name: str, count: int, version: str | None = None, config_file: str | None = None):
    client = docker.from_env()
    aerolab_cmd = [
        "aerolab",
        "cluster",
        "create",
        # "-F=--network={}".format(aql_net),
        "-n",
        name,
        "-c",
        str(count),
    ]

    if config_file:
        aerolab_cmd.append("--customconf={}".format(config_file))
        
    if version:
        aerolab_cmd.append("-v={}".format(version))

    subprocess.run(aerolab_cmd)

    containers = client.containers.list()
    ips = []

    for c in containers:
        if "aerolab-aql" not in c.attrs["Name"]:
            continue

        nets = c.attrs["NetworkSettings"]["Networks"].values()

        for net in nets:
            if "IPAddress" in net:
                ips.append(net["IPAddress"])
                break

    time.sleep(3)  # TODO: Add mechanism to wait until containers can be connected to
    return ips


def shutdown_containers(name: str):
    aerolab_cmd = ["aerolab", "cluster", "destroy", "-n", name, "-f"]
    subprocess.run(aerolab_cmd)
    client = docker.from_env()
    client.networks.prune()

def check_for_valigrind() -> bool:
    return os.path.isfile("valgrind")

def run_aql(args=None) -> subprocess.CompletedProcess:
    cmds = [
        "../target/Linux-x86_64/bin/aql",
        "../target/Darwin-x86_64/bin/aql",
        "target/Linux-x86_64/bin/aql",
        "target/Darwin-x86_64/bin/aql",
    ]
    
    cmd = [cmd for cmd in cmds if os.path.isfile(cmd)]
    args = [] if args is None else args
    cmd.extend(args)
    return subprocess.run(cmd, capture_output=True)


def create_client(seed: tuple[str, int] = ("127.0.0.1", 3000)):
    global as_client
    config = {"hosts": [seed], "timeout": 3}
    as_client = aerospike.client(config)
    print("Successfully connected to seed", seed)


def populate_db(set_name: str):
    global as_client
    write_policy = {"key": aerospike.POLICY_KEY_SEND}
    keys = []

    for idx in range(100):
        key = ("test", set_name, "key" + str(idx))
        bins = {
            "str": str(idx),
            "a-str": str(idx % 10),
            "b-str": str(idx % 5),
            "int": idx % 5,
            "a-int": idx % 5,
            "b-int": idx % 10,
            "float": idx * 3.14,
            "int-str-mix": str(idx % 5) if idx >= 80 else idx % 10,
        }
        keys.append(key)
        as_client.put(key, bins, policy=write_policy)

    print("Successfully populated DB")


def create_sindex(name, type_, ns, bin, set_: str | None = None):
    req = "sindex-create:ns={};indexname={};indexdata={},{}".format(
        ns, name, bin, type_
    )
    
    if (set_):
        req += ";set=" + set_
        
    as_client.info_all(req)
    
    time.sleep(3)  # TODO: Instead of sleep wait for sindex to exist
    print("Successfully created secondary index", name)
    
def delete_sindex(name, ns):
    as_client.info_all(
        "sindex-delete:ns={};indexname={}".format(
            ns, name
        )
    )
    print("Successfully deleted secondary index", name)

def parse_json_output(output: bytes) -> dict[str, Any]:
    try:
        raw_out = output.decode("utf-8")
        cleaned_out = raw_out.split("\n")
        cleaned_out = "\n".join(cleaned_out[4:])
        return json.loads(cleaned_out)
    except Exception as e:
        raise Exception("Unable to parse JSON output. Must be incorrectly formatted: {}".format(e))