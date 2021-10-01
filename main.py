import argparse
import logging
import re
import subprocess
import time
from collections import deque
from os import getenv
from pathlib import Path
from select import select
from urllib.request import urlopen

import pika
import timeout_decorator
import wagglemsg as message
from gpsdclient import GPSDClient
from prometheus_client.parser import text_string_to_metric_families
from pySMART import Device


def get_node_exporter_metrics(url):
    with urlopen(url) as f:
        return f.read().decode()


def get_uptime_seconds():
    text = Path("/host/proc/uptime").read_text()
    fs = text.split()
    return float(fs[0])


# prom2waggle holds a map of node_exporter's metrics to our metrics
prom2waggle = {
    # time
    "node_boot_time_seconds": "sys.boot_time",
    "node_time_seconds": "sys.time",
    # cpu
    "node_cpu_seconds_total": "sys.cpu_seconds",
    # load
    "node_load1": "sys.load1",
    "node_load5": "sys.load5",
    "node_load15": "sys.load15",
    # cpu frequencies
    "node_cpu_scaling_frequency_hertz": "sys.freq.cpu",
    "node_cpu_scaling_frequency_min_hertz": "sys.freq.cpu_min",
    "node_cpu_scaling_frequency_max_hertz": "sys.freq.cpu_max",
    # mem
    "node_memory_MemAvailable_bytes": "sys.mem.avail",
    "node_memory_MemFree_bytes": "sys.mem.free",
    "node_memory_MemTotal_bytes": "sys.mem.total",
    # fs
    "node_filesystem_avail_bytes": "sys.fs.avail",
    "node_filesystem_size_bytes": "sys.fs.size",
    # net
    "node_network_receive_bytes_total": "sys.net.rx_bytes",
    "node_network_receive_packets_total": "sys.net.rx_packets",
    "node_network_transmit_bytes_total": "sys.net.tx_bytes",
    "node_network_transmit_packets_total": "sys.net.tx_packets",
    "node_network_up": "sys.net.up",
    # thermal
    "node_thermal_zone_temp": "sys.thermal",
    "node_hwmon_temp_celsius": "sys.hwmon",
    "node_cooling_device_cur_state": "sys.cooling",
    "node_cooling_device_max_state": "sys.cooling_max",
}

# mapping of gps metric to it's error estimate key
gpsvalue2err = {
    "lat": "epy",
    "lon": "epx",
    "alt": "epv",
}


def __val_freq(val):
    VAL_FRE_RE = re.compile(r"\b(\d+)%@(\d+)")
    if "@" in val:
        match = VAL_FRE_RE.search(val)
        return {"perc": int(match.group(1)), "freq": int(match.group(2)) * 1000000}
    else:
        return {"freq": int(val) * 1000000}


def add_system_metrics_tegra(args, messages):
    """Add system metrics gathered by the `tegrastats` subprocess

    Args:
        args: all program arguments
        messages: the message queue to append metric to
    """
    timestamp = time.time_ns()

    logging.info("collecting system metrics (tegra)")

    tegradata = None
    try:
        with subprocess.Popen(["tegrastats"], stdout=subprocess.PIPE) as process:
            # wait for 10 seconds to get tegrastats info
            pollresults = select([process.stdout], [], [], 10)[0]
            if pollresults:
                output = pollresults[0].readline()
                if output:
                    tegradata = output.strip().decode()

        if tegradata:
            # populate CPU frequency percentages
            ## ex. CPU [25%@652,15%@806,16%@880,31%@902,19%@960,38%@960]
            CPU_RE = re.compile(r"CPU \[(.*?)\]")
            cpudata = CPU_RE.search(tegradata)
            if cpudata:
                for idx, cpu_str in enumerate(cpudata.group(1).split(",")):
                    if "off" == cpu_str:
                        continue

                    messages.append(
                        message.Message(
                            name="sys.freq.cpu_perc",
                            value=__val_freq(cpu_str)["perc"],
                            timestamp=timestamp,
                            meta={"cpu": str(idx)},
                        )
                    )

            # populate the GPU, EMC (external memory controller),
            #  APE (audio processing engine), etc. freqency percentages
            ## ex. EMC_FREQ 1%@1600 GR3D_FREQ 0%@114 APE 150
            VALS_RE = re.compile(r"\b([A-Z0-9_]+) ([0-9%@]+)(?=[^/])\b")
            for name, val in re.findall(VALS_RE, tegradata):
                name = name.split("_")[0] if "FREQ" in name else name
                hz_data = __val_freq(val)

                # normalize to GPU names
                if name.lower() == "gr3d":
                    name = "gpu"

                if hz_data.get("perc", None) is not None:
                    messages.append(
                        message.Message(
                            name="sys.freq.{name}_perc".format(name=name.lower()),
                            value=hz_data["perc"],
                            timestamp=timestamp,
                            meta={},
                        )
                    )

                # ONLY for APE do we report current frequency as it can't
                #  be found more accurate elsewhere
                if name == "APE" and hz_data.get("freq"):
                    messages.append(
                        message.Message(
                            name="sys.freq.{name}".format(name=name.lower()),
                            value=hz_data["freq"],
                            timestamp=timestamp,
                            meta={},
                        )
                    )

            # populate Wattage data (milliwatts)
            ## ex. VDD_IN 5071/4811 VDD_CPU_GPU_CV 1315/1066 VDD_SOC 1116/1116
            WATT_RE = re.compile(r"\b(\w+) ([0-9.]+)\/([0-9.]+)\b")
            for name, current, avg in re.findall(WATT_RE, tegradata):
                messages.append(
                    message.Message(
                        name="sys.power",
                        value=int(current),
                        timestamp=timestamp,
                        meta={"name": name.lower()},
                    )
                )
        else:
            logging.info("tegrastats did not return any data. skipping...")

    except Exception:
        logging.exception("failed to get tegra system metrics")


def add_system_metrics_jetson_clocks(args, messages):
    """Add Jetson specific GPU and EMC frequency information to system metrics

    Args:
        args: all program arguments
        messages: the message queue to append metric to
    """
    timestamp = time.time_ns()

    logging.info("collecting system metrics (Jetson Clocks)")

    pdata = []
    try:
        with subprocess.Popen(["jetson_clocks", "--show"], stdout=subprocess.PIPE) as process:
            # wait for 10 seconds to get jetson_clocks info
            t_end = time.time() + 10
            while time.time() < t_end:
                pollresults = select([process.stdout], [], [], 2)[0]
                if pollresults:
                    output = pollresults[0].readline()
                    if output:
                        pdata.append(output.strip().decode())
                    else:
                        break

        if pdata:
            # populate the GPU and EMC min, max, current frequency
            GPU_RE = re.compile(r"GPU MinFreq=(\d+) MaxFreq=(\d+) CurrentFreq=(\d+)")
            EMC_RE = re.compile(r"EMC MinFreq=(\d+) MaxFreq=(\d+) CurrentFreq=(\d+)")
            for line in pdata:
                gpudata = GPU_RE.search(line)
                emcdata = EMC_RE.search(line)
                name = ""
                if gpudata:
                    name = "gpu"
                    freqdata = gpudata
                elif emcdata:
                    name = "emc"
                    freqdata = emcdata

                if name:
                    messages.append(
                        message.Message(
                            name="sys.freq.{name}_min".format(name=name.lower()),
                            value=int(freqdata.group(1)),
                            timestamp=timestamp,
                            meta={},
                        )
                    )
                    messages.append(
                        message.Message(
                            name="sys.freq.{name}_max".format(name=name.lower()),
                            value=int(freqdata.group(2)),
                            timestamp=timestamp,
                            meta={},
                        )
                    )
                    messages.append(
                        message.Message(
                            name="sys.freq.{name}".format(name=name.lower()),
                            value=int(freqdata.group(3)),
                            timestamp=timestamp,
                            meta={},
                        )
                    )
        else:
            logging.info("jetson_clocks did not return any data. skipping...")

    except Exception:
        logging.exception("failed to get jetson clock system metrics")


def add_system_metrics_nvme(args, messages):
    """Add system metrics for an optional NVMe drive (/dev/nvme0)

    Args:
        args: all program arguments
        messages: the message queue to append metric to
    """
    timestamp = time.time_ns()

    logging.info("collecting system metrics (NVMe)")

    nvmeroot = "/dev/nvme0"
    type = "nvme-therm"
    zone = "none"
    try:
        if Path(nvmeroot).exists():
            nvmedev = Device("/dev/nvme0")
            messages.append(
                message.Message(
                    name="sys.thermal",
                    value=float(nvmedev.temperature),
                    timestamp=timestamp,
                    meta={"type": type, "zone": zone},
                )
            )
        else:
            logging.info("nvme (%s) not found. skipping...", nvmeroot)
    except Exception:
        logging.exception("failed to get nvme system metrics")


@timeout_decorator.timeout(10)
def add_system_metrics_gps(args, messages):
    """Add GPS system metrics

    Args:
        args: all program arguments
        messages: the message queue to append metric to
    """
    timestamp = time.time_ns()

    logging.info("collecting system metrics (GPS)")

    if "nxcore" not in args.waggle_host_id:
        logging.warning("skipping GPS publish for non-main host (%s)", args.waggle_host_id)
        return

    try:
        tpv_report = False
        sat_report = False
        gpsclient = GPSDClient(host=args.gpsd_host, port=args.gpsd_port)
        for result in gpsclient.dict_stream(convert_datetime=False):
            # look for a GPS report that has GPS lock (mode 2 [2D] or 3 [3D])
            if not tpv_report and result["class"] == "TPV" and result["mode"] in [2, 3]:
                tpv_report = True
                for vkey in ["lat", "lon", "alt", "epx", "epy", "epv"]:
                    value = result.get(vkey)
                    if value:
                        messages.append(
                            message.Message(
                                name="sys.gps.{name}".format(name=vkey),
                                value=float(value),
                                timestamp=timestamp,
                                meta={},
                            )
                        )
                    else:
                        logging.info("gps (%s) not found. skipping...", vkey)

            # report salellite info
            if not sat_report and result["class"] == "SKY" and result["satellites"]:
                sat_report = True
                # loop over the sallites and count number being used
                used_sats = len([x for x in result["satellites"] if x["used"]])
                messages.append(
                    message.Message(
                        name="sys.gps.satellites".format(name=vkey),
                        value=int(used_sats),
                        timestamp=timestamp,
                        meta={},
                    )
                )

            if sat_report and tpv_report:
                break

    except Exception:
        logging.exception("failed to get gps system metrics")


def add_system_metrics(args, messages):
    timestamp = time.time_ns()

    logging.info("collecting system metrics from %s", args.metrics_url)
    text = get_node_exporter_metrics(args.metrics_url)

    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            try:
                name = prom2waggle[sample.name]
            except KeyError:
                continue

            messages.append(
                message.Message(
                    name=name,
                    value=sample.value,
                    timestamp=timestamp,
                    meta=sample.labels,
                )
            )

    add_system_metrics_tegra(args, messages)
    add_system_metrics_jetson_clocks(args, messages)
    add_system_metrics_nvme(args, messages)
    add_system_metrics_gps(args, messages)


def add_uptime_metrics(args, messages):
    logging.info("collecting uptime metrics")
    timestamp = time.time_ns()
    try:
        uptime = get_uptime_seconds()
        messages.append(
            message.Message(
                name="sys.uptime",
                value=uptime,
                timestamp=timestamp,
                meta={},
            )
        )
    except FileNotFoundError:
        logging.warning("could not access /host/proc/uptime")
    except Exception:
        logging.exception("failed to get uptime")


def add_version_metrics(args, messages):
    logging.info("collecting version metrics")
    timestamp = time.time_ns()

    try:
        version = Path("/host/etc/waggle_version_os").read_text().strip()
        messages.append(
            message.Message(
                name="sys.version.os",
                value=version,
                timestamp=timestamp,
                meta={},
            )
        )
        logging.info("added os version")
    except FileNotFoundError:
        logging.info("os version not found. skipping...")
    except Exception:
        logging.exception("failed to get os version")


def add_provision_metrics(args, messages):
    logging.info("collecting system provision metrics")
    timestamp = time.time_ns()
    try:
        # check the last line is a complete factory provision log
        lastline = Path("/host/etc/waggle/factory_provision").read_text().strip().rsplit("\n", 1)[1]
        if "Factory Provisioning Finish" in lastline:
            date = lastline.rsplit(":", 1)[0]
            messages.append(
                message.Message(
                    name="sys.provision.factory_date",
                    value=date,
                    timestamp=timestamp,
                    meta={},
                )
            )
        logging.info("added factory provision date")
    except FileNotFoundError:
        logging.info("factory provision not found, skipping...")
    except Exception:
        logging.exception("failed to get factory provision")


def add_metrics_data_dir(args, messages):
    for path in args.metrics_data_dir.glob("*/*"):
        if path.name.startswith("."):
            continue
        try:
            msg = message.load(path.read_text())
            messages.append(msg)
            logging.info("added metric in %s", path)
        except Exception:
            logging.exception("failed to parse metric in %s", path)
        finally:
            # TODO we expect this to work right now. if we can't unlink this then
            # this metric will keep getting queued up
            path.unlink()


def flush_messages_to_rabbitmq(args, messages):
    if len(messages) == 0:
        logging.warning("no metrics queued. skipping publish")
        return

    params = pika.ConnectionParameters(
        host=args.rabbitmq_host,
        port=args.rabbitmq_port,
        credentials=pika.PlainCredentials(
            username=args.rabbitmq_username,
            password=args.rabbitmq_password,
        ),
        connection_attempts=3,
        retry_delay=3.0,
        socket_timeout=3.0,
    )

    logging.info(
        "publishing metrics to rabbitmq server at %s:%d as %s",
        params.host,
        params.port,
        params.credentials.username,
    )

    published_total = 0

    try:
        with pika.BlockingConnection(params) as connection:
            channel = connection.channel()
            while len(messages) > 0:
                msg = messages[0]
                # tag message with node and host metadata
                msg.meta["node"] = args.waggle_node_id
                msg.meta["host"] = args.waggle_host_id
                if args.waggle_node_vsn != "":
                    msg.meta["vsn"] = args.waggle_node_vsn
                # add to rabbitmq queue
                channel.basic_publish(
                    exchange=args.rabbitmq_exchange,
                    routing_key=msg.name,
                    body=message.dump(msg),
                )
                # dequeue message *after* it has been published to rabbtimq
                messages.popleft()
                published_total += 1
    except Exception:
        logging.warning("rabbitmq connection failed. %d metrics buffered for retry", len(messages))

    logging.info("published %d metrics", published_total)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="enable debug logs")
    parser.add_argument(
        "--waggle-node-id",
        default=getenv("WAGGLE_NODE_ID", "0000000000000000"),
        help="waggle node id",
    )
    parser.add_argument(
        "--waggle-node-vsn",
        default=getenv("WAGGLE_NODE_VSN", ""),
        help="waggle node vsn",
    )
    parser.add_argument(
        "--waggle-host-id", default=getenv("WAGGLE_HOST_ID", ""), help="waggle host id"
    )
    parser.add_argument(
        "--rabbitmq-host",
        default=getenv("RABBITMQ_HOST", "localhost"),
        help="rabbitmq host",
    )
    parser.add_argument(
        "--rabbitmq-port",
        default=int(getenv("RABBITMQ_PORT", "5672")),
        type=int,
        help="rabbitmq port",
    )
    parser.add_argument(
        "--rabbitmq-username",
        default=getenv("RABBITMQ_USERNAME", "guest"),
        help="rabbitmq username",
    )
    parser.add_argument(
        "--rabbitmq-password",
        default=getenv("RABBITMQ_PASSWORD", "guest"),
        help="rabbitmq password",
    )
    parser.add_argument(
        "--rabbitmq-exchange",
        default=getenv("RABBITMQ_EXCHANGE", "metrics"),
        help="rabbitmq exchange to publish to",
    )
    parser.add_argument(
        "--gpsd-host",
        default=getenv("GPSD_HOST", "localhost"),
        help="gpsd host",
    )
    parser.add_argument(
        "--gpsd-port",
        default=int(getenv("GPSD_PORT", "2947")),
        type=int,
        help="gpsd port",
    )
    parser.add_argument(
        "--metrics-url",
        default=getenv("METRICS_URL", "http://localhost:9100/metrics"),
        help="node exporter metrics url",
    )
    parser.add_argument(
        "--metrics-collect-interval",
        default=float(getenv("METRICS_COLLECT_INTERVAL", "60.0")),
        type=float,
        help="interval in seconds to collect metrics",
    )
    parser.add_argument(
        "--metrics-data-dir",
        default=getenv("METRICS_DATA_DIR", "/run/metrics"),
        type=Path,
        help="metrics data directory",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )
    # pika logging is too verbose, so we turn it down.
    logging.getLogger("pika").setLevel(logging.CRITICAL)

    logging.info("metrics agent started on %s", args.waggle_host_id)

    messages = deque()

    logging.info("collecting one time startup metrics")
    add_version_metrics(args, messages)
    add_provision_metrics(args, messages)

    logging.info("collecting metrics every %s seconds", args.metrics_collect_interval)
    runtime = 0

    while True:
        sleeptime = (
            0
            if args.metrics_collect_interval - runtime < 0
            else args.metrics_collect_interval - runtime
        )
        logging.info("starting metrics collection in %s seconds", int(sleeptime))
        time.sleep(sleeptime)
        logging.info("starting metrics collection")
        start = time.time()

        try:
            add_metrics_data_dir(args, messages)
        except Exception:
            logging.exception("failed to add data dir metrics")

        try:
            add_system_metrics(args, messages)
        except Exception:
            logging.warning("failed to add system metrics")

        try:
            add_uptime_metrics(args, messages)
        except Exception:
            logging.warning("failed to add uptime metrics")

        flush_messages_to_rabbitmq(args, messages)

        runtime = time.time() - start
        logging.info("finished metrics collection")


if __name__ == "__main__":
    main()
