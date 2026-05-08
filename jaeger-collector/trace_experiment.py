"""
Run Locust workloads and collect Jaeger trace latency summaries.

The script is intended to run from the Kubernetes control node, where both
kubectl and the Jaeger query service are reachable.
"""

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
HELPERS_PATH = REPO_ROOT / "controller-helpers"
sys.path.insert(0, str(HELPERS_PATH))

import appl_graphs


APP_ALIASES = {
    "hotelreservation": "reservation",
    "hotel_reservation": "reservation",
    "reservation": "reservation",
    "socialmedia": "social",
    "social_media": "social",
    "socialnetwork": "social",
    "social_network": "social",
    "social": "social",
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--app", choices=sorted(APP_ALIASES), required=True)
    parser.add_argument("--gateway-url", default="http://10.10.1.1:32000")
    parser.add_argument("--jaeger-ip")
    parser.add_argument("--workload", action="append", required=True)
    parser.add_argument("--replica-set", action="append")
    parser.add_argument("--duration", type=int, default=300)
    parser.add_argument("--warmup", type=int, default=30)
    parser.add_argument("--workers", type=int, default=0)
    parser.add_argument("--multiplier", type=int, default=1)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--out-dir", default="trace-results")
    parser.add_argument("--use-proxy", action="store_true")
    parser.add_argument("--skip-rollout-wait", action="store_true")
    return parser.parse_args()


def normalize_app(app):
    return APP_ALIASES[app]


def get_app_graph(app):
    if app == "reservation":
        return appl_graphs.hotel_reservation
    if app == "social":
        return appl_graphs.social_network
    raise ValueError(f"Unsupported app: {app}")


def get_jaeger_ip():
    result = subprocess.check_output(
        ["kubectl", "get", "svc", "jaeger", "-o", "jsonpath={.spec.clusterIP}"],
        text=True,
    )
    return result.strip()


def parse_replica_set(replica_set):
    if replica_set == "baseline":
        return {}

    replicas = {}
    for item in replica_set.split(","):
        service, count = item.split("=", 1)
        replicas[service.strip()] = int(count)
    return replicas


def apply_replica_set(replica_set, namespace, wait_for_rollout):
    for service, replicas in replica_set.items():
        subprocess.check_call(
            [
                "kubectl",
                "scale",
                "deployment",
                service,
                f"--replicas={replicas}",
                "-n",
                namespace,
            ]
        )

    if not wait_for_rollout:
        return

    for service in replica_set:
        subprocess.check_call(
            [
                "kubectl",
                "rollout",
                "status",
                "deployment",
                service,
                "-n",
                namespace,
                "--timeout=180s",
            ]
        )


def write_rps_file(workload, multiplier, duration, warmup, destination):
    total_seconds = duration + warmup
    if workload.startswith("fixed_"):
        rate = int(workload.split("_", 1)[1])
        values = [rate * multiplier] * total_seconds
    else:
        with open(os.path.expanduser(workload), "r") as f:
            source_values = [int(line.strip()) * multiplier for line in f if line.strip()]
        if len(source_values) == 0:
            raise ValueError(f"Workload file is empty: {workload}")
        repeats = (total_seconds // len(source_values)) + 1
        values = (source_values * repeats)[:total_seconds]

    destination.write_text("\n".join(str(value) for value in values) + "\n")


def locust_args(app, temp_dir, gateway_url, workers, use_proxy):
    locustfile = REPO_ROOT / "client" / f"locust_{app}.py"
    base_args = [
        "locust",
        "--headless",
        "-f",
        str(locustfile),
        "-H",
        gateway_url,
        "--csv",
        str(temp_dir / "locust"),
        "--csv-full-history",
    ]
    if use_proxy:
        base_args.append("--use-proxy=True")

    if workers <= 0:
        return [base_args]

    worker_args = ["locust", "--worker", "-f", str(locustfile)]
    if use_proxy:
        worker_args.append("--use-proxy=True")

    master_args = base_args + ["--master", "--expect-workers", str(workers)]
    return [master_args] + [worker_args for _ in range(workers)]


def run_locust_and_collect(app, args, jaeger_ip, graph, scenario_dir):
    import query_jaeger

    with tempfile.TemporaryDirectory(prefix="galileo-trace-") as temp_name:
        temp_dir = Path(temp_name)
        write_rps_file(
            args.workload_name,
            args.multiplier,
            args.duration,
            args.warmup,
            temp_dir / "rps.txt",
        )

        processes = []
        stdout_files = []
        for index, command in enumerate(
            locust_args(app, temp_dir, args.gateway_url, args.workers, args.use_proxy)
        ):
            stdout_path = scenario_dir / f"locust-{index}.out"
            stdout = open(stdout_path, "w")
            stdout_files.append(stdout)
            processes.append(
                subprocess.Popen(
                    command,
                    cwd=temp_dir,
                    stdout=stdout,
                    stderr=subprocess.STDOUT,
                )
            )

        try:
            time.sleep(args.warmup)
            summary = query_jaeger.get_trace_latency_summary(
                args.duration,
                jaeger_ip,
                graph["frontend_service"],
                [
                    {"name": name, "execution_path": path}
                    for name, path in zip(
                        graph["request_types"]["by_type"],
                        graph["request_types"]["by_service"],
                    )
                ],
            )
        finally:
            for process in processes:
                process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for stdout in stdout_files:
                stdout.close()

        for path in temp_dir.glob("locust*"):
            if path.is_file():
                shutil.copy(path, scenario_dir / path.name)

        return summary


def write_summary_files(summary, scenario_dir):
    json_path = scenario_dir / "jaeger-latency-summary.json"
    json_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")

    csv_path = scenario_dir / "jaeger-latency-summary.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "request_type",
                "scope",
                "service",
                "count",
                "mean_ms",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "max_ms",
            ]
        )
        for request_type, data in summary["request_types"].items():
            e2e = data["e2e"]
            writer.writerow(
                [
                    request_type,
                    "e2e",
                    summary["frontend_service"],
                    e2e["count"],
                    e2e["mean_ms"],
                    e2e["p50_ms"],
                    e2e["p95_ms"],
                    e2e["p99_ms"],
                    e2e["max_ms"],
                ]
            )
            for service, stats in data["services"].items():
                writer.writerow(
                    [
                        request_type,
                        "service",
                        service,
                        stats["count"],
                        stats["mean_ms"],
                        stats["p50_ms"],
                        stats["p95_ms"],
                        stats["p99_ms"],
                        stats["max_ms"],
                    ]
                )


def scenario_name(workload, replica_set):
    safe_workload = workload.replace("/", "_").replace("~", "home")
    safe_replicas = replica_set.replace(",", "__").replace("=", "-")
    return f"{safe_workload}__{safe_replicas}"


def main():
    args = parse_args()
    app = normalize_app(args.app)
    graph = get_app_graph(app)
    jaeger_ip = args.jaeger_ip or get_jaeger_ip()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    replica_sets = args.replica_set or ["baseline"]

    for workload in args.workload:
        for replica_set_name in replica_sets:
            replica_set = parse_replica_set(replica_set_name)
            scenario_dir = out_dir / scenario_name(workload, replica_set_name)
            scenario_dir.mkdir(parents=True, exist_ok=True)

            if replica_set:
                apply_replica_set(
                    replica_set,
                    args.namespace,
                    wait_for_rollout=not args.skip_rollout_wait,
                )

            args.workload_name = workload
            summary = run_locust_and_collect(app, args, jaeger_ip, graph, scenario_dir)
            summary["app"] = app
            summary["workload"] = workload
            summary["replicas"] = replica_set
            summary["jaeger_ip"] = jaeger_ip
            write_summary_files(summary, scenario_dir)

            print(
                f"Wrote Jaeger latency summary for workload={workload}, "
                f"replicas={replica_set_name} to {scenario_dir}",
                flush=True,
            )


if __name__ == "__main__":
    main()
