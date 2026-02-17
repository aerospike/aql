import atexit
import codecs
import json
import os
import signal
import string
import subprocess
import sys
import time
from typing import Any

import aerospike
import docker

SET_NAME = "aql-tests"
AEROSPIKE_VERSION = "latest"
PORT = 10000
SERVER_IP = "127.0.0.1"
CONTAINER_NAME = "aql-test-server"
CLIENT_ATTEMPTS = 20

WORK_DIRECTORY = "work"
STATE_DIRECTORY = "state-1"
CONTAINER_DIR = "/opt/work"

# Initialized lazily to avoid failures during pytest discovery
DOCKER_CLIENT = None
CONTAINER = None
as_client = None


def _get_docker_client():
    global DOCKER_CLIENT

    if DOCKER_CLIENT is None:
        try:
            DOCKER_CLIENT = docker.from_env()
        except Exception as e:
            raise RuntimeError(
                "Failed to connect to Docker. Is Docker running? Error: {}".format(e)
            )

    return DOCKER_CLIENT


def absolute_path(*path):
    if len(path) == 1 and os.path.isabs(path[0]):
        return path[0]
    return os.path.abspath(os.path.join(os.path.dirname(__file__), *path))


def _init_work_dir():
    work = absolute_path(WORK_DIRECTORY)
    state = absolute_path(WORK_DIRECTORY, STATE_DIRECTORY)
    smd = os.path.join(state, "smd")

    for d in [work, state, smd]:
        os.makedirs(d, exist_ok=True)


def _remove_work_dir():
    work = absolute_path(WORK_DIRECTORY)
    if os.path.exists(work):
        import shutil
        shutil.rmtree(work, ignore_errors=True)


def _create_conf_file(port_base, access_address="127.0.0.1"):
    template_file = absolute_path("aerospike.conf")
    with codecs.open(template_file, "r", "UTF-8") as f:
        template_content = f.read()

    params = {
        "state_directory": CONTAINER_DIR + "/" + STATE_DIRECTORY,
        "service_port": str(port_base),
        "fabric_port": str(port_base + 1),
        "heartbeat_port": str(port_base + 2),
        "info_port": str(port_base + 3),
        "access_address": access_address,
    }

    temp = string.Template(template_content)
    conf_content = temp.substitute(params)

    conf_file = absolute_path(WORK_DIRECTORY, "aerospike.conf")
    with codecs.open(conf_file, "w", "UTF-8") as f:
        f.write(conf_content)

    return conf_file


def run_containers(
    name: str,
    count: int,
    version: str | None = None,
    config_file: str | None = None,
):
    global CONTAINER

    client = _get_docker_client()
    version = version or AEROSPIKE_VERSION
    image = "aerospike/aerospike-server:{}".format(version)

    _init_work_dir()

    mount_dir = absolute_path(WORK_DIRECTORY)
    conf_file = _create_conf_file(PORT)
    conf_name = os.path.basename(conf_file)

    cmd = "/usr/bin/asd --foreground --config-file {}/{}".format(
        CONTAINER_DIR, conf_name
    )

    # Remove existing container if present
    try:
        old = client.containers.get(CONTAINER_NAME)
        old.remove(force=True)
    except:
        pass

    try:
        print("Pulling image: {}".format(image))
        client.images.pull(image)
        print("Pulled image: {}".format(image))
    except:
        pass

    print("Starting container: {}".format(cmd))

    CONTAINER = client.containers.run(
        image,
        command=cmd,
        ports={
            str(PORT) + "/tcp": str(PORT),
            str(PORT + 1) + "/tcp": str(PORT + 1),
            str(PORT + 2) + "/tcp": str(PORT + 2),
            str(PORT + 3) + "/tcp": str(PORT + 3),
        },
        volumes={mount_dir: {"bind": CONTAINER_DIR, "mode": "rw"}},
        tty=True,
        detach=True,
        name=CONTAINER_NAME,
    )

    _wait_for_server()

    return [SERVER_IP]


def _wait_for_server(timeout=30):
    """Wait for Aerospike server to accept connections."""
    start = time.time()

    while time.time() - start < timeout:
        try:
            config = {"hosts": [(SERVER_IP, PORT)], "timeout": 1000}
            client = aerospike.client(config).connect()
            client.close()
            print("Aerospike server is ready")
            return
        except Exception:
            time.sleep(1)

    raise Exception(
        "Aerospike server did not become ready within {} seconds".format(timeout)
    )


def shutdown_containers(name: str = None):
    global CONTAINER

    if CONTAINER is not None:
        try:
            CONTAINER.stop()
            CONTAINER.remove()
        except:
            pass
        CONTAINER = None

    _remove_work_dir()

    try:
        _get_docker_client().networks.prune()
    except:
        pass


def check_for_valgrind() -> bool:
    return os.path.isfile("valgrind")


def run_aql(args=None) -> subprocess.CompletedProcess:
    cmds = [
        "../target/Linux-x86_64/bin/aql",
        "../target/Darwin-x86_64/bin/aql",
        "../target/Darwin-arm64/bin/aql",
        "target/Linux-x86_64/bin/aql",
        "target/Darwin-x86_64/bin/aql",
        "target/Darwin-arm64/bin/aql",
    ]

    cmd = [cmd for cmd in cmds if os.path.isfile(cmd)]
    args = [] if args is None else args
    cmd.extend(args)
    return subprocess.run(cmd, capture_output=True)


def create_client(seed: tuple[str, int] = None):
    global as_client

    if seed is None:
        seed = (SERVER_IP, PORT)

    config = {"hosts": [seed], "timeout": 3000}

    for attempt in range(CLIENT_ATTEMPTS):
        try:
            as_client = aerospike.client(config).connect()
            print("Successfully connected to seed", seed)
            return
        except Exception:
            if attempt < CLIENT_ATTEMPTS - 1:
                time.sleep(1)
            else:
                raise


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
    as_client.info_all(
        "sindex-create:ns={};indexname={};indexdata={},{}{}".format(
            ns,
            name,
            bin,
            type_,
            ";set=" + set_ if set_ else "",
        )
    )

    time.sleep(3)  # TODO: Instead of sleep wait for sindex to exist
    print("Successfully created secondary index", name)


def delete_sindex(name, ns):
    as_client.info_all(
        "sindex-delete:ns={};indexname={}".format(ns, name)
    )
    print("Successfully deleted secondary index", name)


def parse_json_output(output: bytes) -> dict[str, Any]:
    try:
        raw_out = output.decode("utf-8")
        cleaned_out = raw_out.split("\n")
        cleaned_out = "\n".join(cleaned_out[4:])
        return json.loads(cleaned_out)
    except Exception as e:
        raise Exception(
            "Unable to parse JSON output. Must be incorrectly formatted: {}".format(e)
        )


def _graceful_exit(sig, frame):
    shutdown_containers()
    sys.exit(1)


def _stop_silent():
    stdout_tmp = sys.stdout
    stderr_tmp = sys.stderr
    null = open(os.devnull, "w")
    sys.stdout = null
    sys.stderr = null
    try:
        shutdown_containers()
        sys.stdout = stdout_tmp
        sys.stderr = stderr_tmp
    except:
        sys.stdout = stdout_tmp
        sys.stderr = stderr_tmp


signal.signal(signal.SIGINT, _graceful_exit)
atexit.register(_stop_silent)
