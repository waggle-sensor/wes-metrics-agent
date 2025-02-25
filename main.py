import argparse
import logging
import re
import sched
import signal
import subprocess
import time
from collections import deque
from os import getenv
from pathlib import Path
from queue import Empty, Queue
from threading import Thread
from urllib.request import urlopen

import pika
import timeout_decorator
import wagglemsg as message
from gpsdclient import GPSDClient
from prometheus_client.parser import text_string_to_metric_families
from pySMART import Device

tegrastats_queue = Queue()
jetsonclocks_queue = Queue()


def get_prometheus_metrics(url):
    """
    Fetches the prometheus metrics from the url.

    Args:
        url: the url of the prometheus metrics endpoint
    Returns:
        the metrics as a string
    """
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
    # Mem Buffers and Cache
    "node_memory_Buffers_bytes": "sys.mem.buffers",
    "node_memory_Cached_bytes":  "sys.mem.cached",
    # Memory Activeness
    "node_memory_Active_bytes": "sys.mem.active",
    "node_memory_Active_anon_bytes": "sys.mem.active.anon",
    "node_memory_Active_file_bytes": "sys.mem.active.file",
    "node_memory_Inactive_bytes": "sys.mem.inactive",
    "node_memory_Inactive_anon_bytes": "sys.mem.inactive.anon",
    "node_memory_Inactive_file_bytes": "sys.mem.inactive.file",
    # Mem writeback Into the Disk
    "node_memory_Dirty_bytes": "sys.mem.dirty",
    "node_memory_Writeback_bytes": "sys.mem.writeback",
    "node_memory_WritebackTmp_bytes": "sys.mem.writebacktmp",
    # Mapped memory
    "node_memory_AnonPages_bytes": "sys.mem.anon_pages",
    "node_memory_Mapped_bytes": "sys.mem.mapped",
    # Shared memory
    "node_memory_Shmem_bytes": "sys.mem.shared",
    # Kernel memory
    "node_memory_Slab_bytes": "sys.mem.slab",
    "node_memory_SReclaimable_bytes": "sys.mem.sreclaimable",
    "node_memory_SUnreclaim_bytes": "sys.mem.sunreclaim",
    "node_memory_KernelStack_bytes": "sys.mem.kernel_stack",
    # Allocation Mem Availability
    "node_memory_CommitLimit_bytes": "sys.mem.commit_limit",
    "node_memory_Committed_AS_bytes": "sys.mem.committed_as",
    # Virtual Memory
    "node_memory_PageTables_bytes": "sys.mem.page_tables",
    "node_memory_VmallocTotal_bytes": "sys.mem.vmalloc_total",
    "node_memory_VmallocUsed_bytes": "sys.mem.vmalloc_used",
    "node_memory_VmallocChunk_bytes": "sys.mem.vmalloc_chunk",
    # Others mem parameters
    "node_memory_Unevictable_bytes": "sys.mem.unevictable",
    "node_memory_Mlocked_bytes": "sys.mem.mlocked",
    "node_memory_NFS_Unstable_bytes": "sys.mem.nfs_unstable",
    "node_memory_Bounce_bytes": "sys.mem.bounce",
    "node_memory_CmaFree_bytes": "sys.mem.cma_free",
    "node_memory_CmaTotal_bytes": "sys.mem.cma_total",
    # swap
    "node_memory_SwapCached_bytes": "sys.mem.swap.cached",
    "node_memory_SwapFree_bytes": "sys.mem.swap.free",
    "node_memory_SwapTotal_bytes": "sys.mem.swap.total",
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
    # ChirpStack Gateway Bridge (lorawan gateway)
        # HELP The percentage of upstream datagrams that were acknowledged.
        # TYPE gauge
    "backend_semtechdup_gateway_ack_rate": "sys.lora.gateway.ack_rate",
        # HELP The number of ack-rates reported.
        # TYPE counter
    "backend_semtechudp_gateway_ack_rate_count": "sys.lora.gateway.ack_rate_count",
        # HELP The number of gateway connections received by the backend.
        # TYPE counter
    "backend_semtechudp_gateway_connect_count": "sys.lora.gateway.connect_count",
        # HELP The number of gateways that disconnected from the backend.
        # TYPE counter
    "backend_semtechudp_gateway_diconnect_count": "sys.lora.gateway.disconnect_count",
        # HELP The number of UDP packets received by the backend (per packet_type).
        # TYPE counter
    "backend_semtechudp_udp_received_count": "sys.lora.gateway.udp_received_count",
        # HELP The number of UDP packets sent by the backend (per packet_type).
        # TYPE counter
    "backend_semtechudp_udp_sent_count": "sys.lora.gateway.udp_sent_count",
    # ChirpStack Server (lorawan network server)
        # HELP gateway_backend_mqtt_events Number of events received.
        # TYPE gateway_backend_mqtt_events counter
    "gateway_backend_mqtt_events_total": "sys.lora.server.gateway_backend_mqtt_events",
        # HELP uplink_count Number of received uplinks (after deduplication).
        # TYPE uplink_count counter
    "uplink_count_total": "sys.lora.server.uplink_count",
        # HELP gateway_backend_mqtt_commands Number of commands sent.
        # TYPE gateway_backend_mqtt_commands counter,
    "gateway_backend_mqtt_commands_total": "sys.lora.server.gateway_backend_mqtt_commands",
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


def tegrastats_worker(interval):
    """Thread worker to run `tegrastats` and populate queue

    Args:
        interval: data collection interval in seconds
    """
    interval = interval * 1000
    logging.info(f"starting tegrastats worker [{interval} ms]...")

    try:
        with subprocess.Popen(
            ["tegrastats", "--interval", f"{interval}"],
            stdout=subprocess.PIPE,
        ) as process:
            while True:
                output = process.stdout.readline()
                if output:
                    line = output.strip().decode()
                    tegrastats_queue.put((time.time_ns(), line))
                    logging.debug(f"- added to tegrastats queue [size: {tegrastats_queue.qsize()}]")
                    logging.debug(line)
    except FileNotFoundError:
        # we are most likely not running on a jetson device
        logging.warning("'tegrastats' not found, skipping tegrastats collection")
    finally:
        logging.info("stopped tegrastats worker")


def add_system_metrics_tegra(args, messages):
    """Add system metrics gathered by the `tegrastats` subprocess

    Args:
        args: all program arguments
        messages: the message queue to append metric to
    """
    logging.info("collecting system metrics (tegra)")

    # process all queued tegrastats
    while True:
        try:
            timestamp, tegradata = tegrastats_queue.get(block=False)
            logging.debug(
                f"- pulled from tegrastats queue [ts: {timestamp} | q_size: {tegrastats_queue.qsize()}]"
            )
            logging.debug(tegradata)

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

        except Empty:
            break
        except Exception:
            logging.exception("failed to get tegra system metrics")


def jetson_clocks_worker(interval):
    """Thread worker to run `jetson_clocks` and populate queue

    Args:
        interval: data collection interval in seconds
    """
    logging.info(f"starting jetson_clocks worker [{interval} s]...")

    sch = sched.scheduler(time.time, time.sleep)

    def jetson_clocks_runner(delay):
        logging.debug("- scheduling next jetson_clocks to start in %s seconds", int(delay))
        future_event = sch.enter(delay, 0, jetson_clocks_runner, kwargs={"delay": delay})

        pdata = []
        try:
            with subprocess.Popen(["jetson_clocks", "--show"], stdout=subprocess.PIPE) as process:
                while True:
                    output = process.stdout.readline()
                    if output:
                        pdata.append(output.strip().decode())
                    else:
                        break
        except FileNotFoundError:
            # we are most likely not running on a jetson device
            logging.warning("'jetson_clocks' not found, skipping jetson clocks collection")
            sch.cancel(future_event)
            return

        jetsonclocks_queue.put((time.time_ns(), pdata))
        logging.debug(f"- added to jetson_clocks queue [size: {jetsonclocks_queue.qsize()}]")
        logging.debug(pdata)

    sch.enter(interval, 0, jetson_clocks_runner, kwargs={"delay": interval})

    # wait here until all schedules finish (i.e. run forever)
    sch.run()
    logging.info("stopped jetson_clocks worker")


def add_system_metrics_jetson_clocks(args, messages):
    """Add Jetson specific GPU and EMC frequency information to system metrics

    Args:
        args: all program arguments
        messages: the message queue to append metric to
    """
    logging.info("collecting system metrics (Jetson Clocks)")

    # process all queued jetson clocks
    while True:
        try:
            timestamp, pdata = jetsonclocks_queue.get(block=False)
            logging.debug(
                f"- pulled from jetson_clocks queue [ts: {timestamp} | q_size: {jetsonclocks_queue.qsize()}]"
            )
            logging.debug(pdata)

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

        except Empty:
            break
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
            # look for a GPS report
            if not tpv_report and result["class"] == "TPV":
                tpv_report = True
                for vkey in ["lat", "lon", "alt", "epx", "epy", "epv", "mode"]:
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
                        name="sys.gps.satellites",
                        value=int(used_sats),
                        timestamp=timestamp,
                        meta={},
                    )
                )

            if sat_report and tpv_report:
                break

    except Exception:
        logging.exception("failed to get gps system metrics")


@timeout_decorator.timeout(10)
def add_system_metrics(args, messages):
    timestamp = time.time_ns()

    logging.info("collecting system metrics from %s", args.metrics_url)
    text = get_prometheus_metrics(args.metrics_url)

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

@timeout_decorator.timeout(10)
def add_chirpstack_server_metrics(args, messages):
    """Collect and publish Prometheus metrics from the ChirpStack server."""
    timestamp = time.time_ns()
    
    logging.info("collecting ChirpStack server metrics from %s", args.chirpstack_metrics_url)
    text = get_prometheus_metrics(args.chirpstack_metrics_url)
        
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

@timeout_decorator.timeout(10)
def add_chirpstack_gateway_bridge_metrics(args, messages):
    """Collect and publish Prometheus metrics from the ChirpStack gateway bridge."""
    timestamp = time.time_ns()

    logging.info("collecting ChirpStack gateway bridge metrics from %s", args.chirpstack_gateway_metrics_url)
    text = get_prometheus_metrics(args.chirpstack_gateway_metrics_url)

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
                # For non-ChirpStack metrics, add host metadata.
                if not msg.name.startswith("sys.lora"):
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
    parser.add_argument(
        "--chirpstack-metrics-url",
        default=getenv("CHIRPSTACK_METRICS_URL", "http://wes-chirpstack-server:9100/metrics"),
        help="chirpstack server metrics url",
    )
    parser.add_argument(
        "--chirpstack-gateway-metrics-url",
        default=getenv("CHIRPSTACK_GATEWAY_METRICS_URL", "http://wes-chirpstack-gateway-bridge:9100/metrics"),
        help="chirpstack gateway bridge metrics url",
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

    logging.info("starting threaded processes")
    tegra_thread = Thread(
        target=tegrastats_worker, args=(args.metrics_collect_interval,), daemon=True
    )
    tegra_thread.start()
    jetson_thread = Thread(
        target=jetson_clocks_worker, args=(args.metrics_collect_interval,), daemon=True
    )
    jetson_thread.start()

    logging.info("collecting one time startup metrics")
    add_version_metrics(args, messages)
    add_provision_metrics(args, messages)

    logging.info("collecting metrics every %s seconds", args.metrics_collect_interval)
    sch = sched.scheduler(time.time, time.sleep)

    def main_runner(delay):
        logging.info("scheduling next metrics collection to start in %s seconds", int(delay))
        sch.enter(delay, 0, main_runner, kwargs={"delay": delay})

        logging.info("starting metrics collection")
        try:
            add_metrics_data_dir(args, messages)
        except Exception:
            logging.exception("failed to add data dir metrics")

        try:
            add_system_metrics(args, messages)
        except Exception:
            logging.warning("failed to add system metrics")

        try:
            add_system_metrics_tegra(args, messages)
        except Exception:
            logging.warning("failed to add system metrics (tegra)")

        try:
            add_system_metrics_jetson_clocks(args, messages)
        except Exception:
            logging.warning("failed to add system metrics (jetson)")

        try:
            add_system_metrics_nvme(args, messages)
        except Exception:
            logging.warning("failed to add system metrics (nvme)")

        try:
            add_system_metrics_gps(args, messages)
        except Exception:
            logging.warning("failed to add system metrics (gps)")

        try:
            add_uptime_metrics(args, messages)
        except Exception:
            logging.warning("failed to add uptime metrics")

        try:
            add_chirpstack_server_metrics(args, messages)
        except Exception:
            logging.warning("failed to add ChirpStack server metrics")

        try:
            add_chirpstack_gateway_bridge_metrics(args, messages)
        except Exception:
            logging.warning("failed to add ChirpStack gateway bridge metrics")

        flush_messages_to_rabbitmq(args, messages)

        # touch health file for liveness prob
        Path("/tmp/healthy").touch()

        logging.info("finished metrics collection")

    sch.enter(
        args.metrics_collect_interval,
        0,
        main_runner,
        kwargs={"delay": args.metrics_collect_interval},
    )
    # wait here until all schedules finish (i.e. run forever)
    sch.run()


if __name__ == "__main__":
    main()
