#!/usr/bin/env python3
"""Simple MySQL InnoDB ClusterSet monitor.

Design goals:
- Keep CLI small and operational.
- Fetch current status from mysqlsh.
- Compare with previous run via --state-file.
- Send clean email alerts (WARNING/CRITICAL) with severity-based recipients.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

OK = 0
WARNING = 1
CRITICAL = 2
UNKNOWN = 3
SCRIPT_VERSION = "2026.03.12-slim"


@dataclass
class Alert:
    severity: str
    message: str


@dataclass
class Snapshot:
    clusterset_name: str = "unknown"
    domain_name: str = "unknown"
    global_status: str = "unknown"
    primary_cluster: str = "unknown"
    global_primary_instance: str = "unknown"
    clusters: dict[str, dict[str, Any]] = field(default_factory=dict)


class CliParser(argparse.ArgumentParser):
    def format_help(self) -> str:
        base = super().format_help().rstrip()
        return base + f"\n\nScript version: {SCRIPT_VERSION}\nRuntime file: {Path(__file__).resolve()}\n"


def normalize_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        print("UNKNOWN: JSON root is not an object")
        sys.exit(UNKNOWN)

    info = payload.get("info")
    if isinstance(info, str):
        try:
            parsed = json.loads(info)
        except json.JSONDecodeError as exc:
            print(f"UNKNOWN: invalid nested JSON in 'info': {exc}")
            sys.exit(UNKNOWN)
        if isinstance(parsed, dict):
            return parsed

    return payload


def fetch_current_status(mysqlsh_bin: str, mysqlsh_cmd: str, mysqlsh_target: str, mysqlsh_password: str | None) -> dict[str, Any]:
    js = f"print(JSON.stringify({mysqlsh_cmd}))"
    cmd = [mysqlsh_bin, mysqlsh_target, "--js", "-e", js]
    env = dict(os.environ)
    if mysqlsh_password:
        env["MYSQLSH_PASSWORD"] = mysqlsh_password

    try:
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True, env=env)
    except OSError as exc:
        print(f"UNKNOWN: failed to execute mysqlsh: {exc}")
        sys.exit(UNKNOWN)

    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        print(f"UNKNOWN: mysqlsh command failed: {err}")
        sys.exit(UNKNOWN)

    output = (proc.stdout or "").strip()
    if not output:
        print("UNKNOWN: mysqlsh returned empty output")
        sys.exit(UNKNOWN)

    try:
        payload = json.loads(output)
    except json.JSONDecodeError:
        try:
            payload = json.loads(output.splitlines()[-1].strip())
        except json.JSONDecodeError as exc:
            print(f"UNKNOWN: mysqlsh output is not valid JSON: {exc}")
            sys.exit(UNKNOWN)

    return normalize_payload(payload)


def load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"UNKNOWN: file not found: {path}")
        sys.exit(UNKNOWN)
    except json.JSONDecodeError as exc:
        print(f"UNKNOWN: invalid JSON in {path}: {exc}")
        sys.exit(UNKNOWN)
    return normalize_payload(payload)


def build_snapshot(payload: dict[str, Any]) -> Snapshot:
    clusters_raw = payload.get("clusters")
    if not isinstance(clusters_raw, dict):
        clusters_raw = {}

    clusters: dict[str, dict[str, Any]] = {}
    for cname, cdata in clusters_raw.items():
        if not isinstance(cdata, dict):
            continue
        rep = cdata.get("clusterSetReplication") if isinstance(cdata.get("clusterSetReplication"), dict) else {}
        topology = cdata.get("topology") if isinstance(cdata.get("topology"), dict) else {}

        clusters[cname] = {
            "status": cdata.get("status", "unknown"),
            "globalStatus": cdata.get("globalStatus", "unknown"),
            "clusterRole": cdata.get("clusterRole", "unknown"),
            "clusterSetReplicationStatus": cdata.get("clusterSetReplicationStatus", "unknown"),
            "applierStatus": rep.get("applierStatus", "unknown"),
            "receiverStatus": rep.get("receiverStatus", "unknown"),
            "replicationSource": rep.get("source", "unknown"),
            "transactionSetConsistencyStatus": cdata.get("transactionSetConsistencyStatus", "unknown"),
            "transactionSetErrantGtidSet": cdata.get("transactionSetErrantGtidSet", ""),
            "transactionSetMissingGtidSet": cdata.get("transactionSetMissingGtidSet", ""),
            "topology_keys": sorted(topology.keys()),
            "topology": topology,
        }

    return Snapshot(
        clusterset_name=payload.get("clusterSetName") or payload.get("name") or payload.get("domainName") or "unknown",
        domain_name=payload.get("domainName") or "unknown",
        global_status=payload.get("status") or payload.get("globalStatus") or "unknown",
        primary_cluster=payload.get("primaryCluster") or "unknown",
        global_primary_instance=payload.get("globalPrimaryInstance") or "unknown",
        clusters=clusters,
    )


def parse_lag(v: Any) -> float | None:
    if v in (None, ""):
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def assess_snapshot(snapshot: Snapshot, sync_lag_threshold: float, async_lag_threshold: float) -> list[Alert]:
    alerts: list[Alert] = []
    healthy = {"HEALTHY", "OK", "AVAILABLE"}

    if str(snapshot.global_status).upper() not in healthy:
        alerts.append(Alert("CRITICAL", f"ClusterSet global status is {snapshot.global_status}"))

    for cname, cinfo in snapshot.clusters.items():
        if str(cinfo.get("status", "unknown")).upper() not in healthy:
            alerts.append(Alert("CRITICAL", f"Cluster {cname} status is {cinfo.get('status')}"))
        if str(cinfo.get("globalStatus", "unknown")).upper() not in healthy:
            alerts.append(Alert("CRITICAL", f"Cluster {cname} globalStatus is {cinfo.get('globalStatus')}"))

        if str(cinfo.get("clusterRole", "")).upper() == "REPLICA":
            if str(cinfo.get("clusterSetReplicationStatus", "unknown")).upper() != "OK":
                alerts.append(Alert("CRITICAL", f"Replica cluster {cname} clusterSetReplicationStatus={cinfo.get('clusterSetReplicationStatus')}"))
            if str(cinfo.get("applierStatus", "unknown")).upper() != "APPLIED_ALL":
                alerts.append(Alert("CRITICAL", f"Replica cluster {cname} applierStatus={cinfo.get('applierStatus')}"))
            if str(cinfo.get("receiverStatus", "unknown")).upper() != "ON":
                alerts.append(Alert("CRITICAL", f"Replica cluster {cname} receiverStatus={cinfo.get('receiverStatus')}"))
            if cinfo.get("replicationSource") != snapshot.global_primary_instance:
                alerts.append(Alert("WARNING", f"Replica cluster {cname} source={cinfo.get('replicationSource')} (expected {snapshot.global_primary_instance})"))
            if str(cinfo.get("transactionSetConsistencyStatus", "unknown")).upper() != "OK":
                alerts.append(Alert("CRITICAL", f"Replica cluster {cname} transactionSetConsistencyStatus={cinfo.get('transactionSetConsistencyStatus')}"))
            if str(cinfo.get("transactionSetMissingGtidSet", "")).strip():
                alerts.append(Alert("CRITICAL", f"Replica cluster {cname} has missing GTIDs: {cinfo.get('transactionSetMissingGtidSet')}"))
            if str(cinfo.get("transactionSetErrantGtidSet", "")).strip():
                alerts.append(Alert("CRITICAL", f"Replica cluster {cname} has errant GTIDs: {cinfo.get('transactionSetErrantGtidSet')}"))

        topology = cinfo.get("topology") if isinstance(cinfo.get("topology"), dict) else {}
        if not topology:
            alerts.append(Alert("WARNING", f"Cluster {cname} has empty topology"))

        for member, minfo in topology.items():
            if not isinstance(minfo, dict):
                continue
            role = str(minfo.get("memberRole", "unknown"))
            mode = str(minfo.get("mode", "unknown"))
            status = str(minfo.get("status", "unknown"))

            if status.upper() != "ONLINE":
                alerts.append(Alert("CRITICAL", f"Cluster {cname} member {member} status={status}"))

            is_primary_cluster = str(cinfo.get("clusterRole", "")).upper() == "PRIMARY"
            if is_primary_cluster and role.upper() == "PRIMARY" and mode.upper() != "R/W":
                alerts.append(Alert("CRITICAL", f"Primary cluster {cname} writer member {member} mode={mode} (expected R/W)"))
            if role.upper() == "SECONDARY" and mode.upper() != "R/O":
                alerts.append(Alert("WARNING", f"Cluster {cname} secondary member {member} mode={mode} (expected R/O)"))

            lag_immediate = parse_lag(minfo.get("replicationLagFromImmediateSource"))
            if lag_immediate is None:
                alerts.append(Alert("WARNING", f"Cluster {cname} member {member} has non-numeric immediate lag"))
            elif lag_immediate > sync_lag_threshold:
                alerts.append(Alert("WARNING", f"Cluster {cname} member {member} immediate lag {lag_immediate}s > {sync_lag_threshold}s"))

            lag_original = parse_lag(minfo.get("replicationLagFromOriginalSource"))
            if lag_original is None:
                alerts.append(Alert("WARNING", f"Cluster {cname} member {member} has non-numeric original lag"))
            elif lag_original > async_lag_threshold:
                alerts.append(Alert("CRITICAL", f"Cluster {cname} member {member} original-source lag {lag_original}s > {async_lag_threshold}s"))

    return alerts


def compare_snapshots(current: Snapshot, previous: Snapshot) -> list[Alert]:
    alerts: list[Alert] = []
    if current.primary_cluster != previous.primary_cluster:
        alerts.append(Alert("CRITICAL", f"Primary cluster changed ({previous.primary_cluster} -> {current.primary_cluster})"))

    prev_names = set(previous.clusters.keys())
    curr_names = set(current.clusters.keys())

    removed = sorted(prev_names - curr_names)
    added = sorted(curr_names - prev_names)
    if removed:
        alerts.append(Alert("CRITICAL", f"Clusters removed: {', '.join(removed)}"))
    if added:
        alerts.append(Alert("WARNING", f"Clusters added: {', '.join(added)}"))

    for cname in sorted(prev_names & curr_names):
        p = previous.clusters[cname]
        c = current.clusters[cname]
        if c.get("status") != p.get("status"):
            alerts.append(Alert("CRITICAL", f"Cluster {cname} status changed ({p.get('status')} -> {c.get('status')})"))
        if c.get("clusterRole") != p.get("clusterRole"):
            alerts.append(Alert("WARNING", f"Cluster {cname} role changed ({p.get('clusterRole')} -> {c.get('clusterRole')})"))

        p_nodes = set(p.get("topology_keys", []))
        c_nodes = set(c.get("topology_keys", []))
        if p_nodes != c_nodes:
            alerts.append(Alert("WARNING", f"Cluster {cname} topology changed (removed={sorted(p_nodes-c_nodes)}, added={sorted(c_nodes-p_nodes)})"))

    return alerts


def load_recipients(path: Path) -> list[str]:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        print(f"UNKNOWN: recipients file not found: {path}")
        sys.exit(UNKNOWN)

    recipients = [x.strip() for x in raw.split(",") if x.strip()]
    if not recipients:
        print(f"UNKNOWN: recipients file is empty: {path}")
        sys.exit(UNKNOWN)
    return recipients


def extract_affected(alerts: list[Alert]) -> tuple[list[str], list[str]]:
    clusters: set[str] = set()
    members: set[str] = set()
    pm = re.compile(r"Cluster\s+(\S+)\s+member\s+(\S+)")
    pc = re.compile(r"(?:Replica\s+cluster|Primary\s+cluster|Cluster)\s+(\S+)")
    for a in alerts:
        for c, m in pm.findall(a.message):
            clusters.add(c)
            members.add(m)
        for c in pc.findall(a.message):
            clusters.add(c)
    return sorted(clusters), sorted(members)


def build_email_content(alerts: list[Alert], rc: int, snapshot: Snapshot, env_label: str, service_name: str, top_n: int) -> tuple[str, str]:
    is_critical = rc == CRITICAL
    severity = "CRITICAL" if is_critical else "WARNING"
    name = snapshot.clusterset_name if snapshot.clusterset_name != "unknown" else snapshot.domain_name
    if name == "unknown":
        name = snapshot.primary_cluster
    subject = f"[MySQL ClusterSet] {name}{' - ACTION REQUIRED' if is_critical else ''}"

    clusters, members = extract_affected(alerts)
    top_alerts = alerts[: max(1, top_n)]

    action = (
        "Immediate action required by DBA on-call. Verify replication, member status, and failover readiness."
        if is_critical
        else "Please review and monitor. Investigate if the same warning repeats in consecutive runs."
    )

    lines = [
        f"Team: {service_name}",
        f"Environment: {env_label}",
        f"Generated UTC: {datetime.now(timezone.utc).isoformat()}",
        "",
        "Summary",
        "-------",
        f"Severity: {severity}",
        f"ClusterSet: {name}",
        f"Global status: {snapshot.global_status}",
        f"Current primary cluster: {snapshot.primary_cluster}",
        f"Global primary instance: {snapshot.global_primary_instance}",
        f"Total triggered checks: {len(alerts)}",
        "",
        "Affected cluster/member",
        "-----------------------",
        f"Clusters: {', '.join(clusters) if clusters else 'N/A'}",
        f"Members: {', '.join(members) if members else 'N/A'}",
        "",
        "Triggered checks",
        "----------------",
    ]
    lines.extend([f"- {a.severity}: {a.message}" for a in alerts])
    lines.extend([
        "",
        "Current primary",
        "---------------",
        f"Primary cluster: {snapshot.primary_cluster}",
        f"Primary instance: {snapshot.global_primary_instance}",
        "",
        f"Top {max(1, top_n)} errors",
        "------------",
    ])
    lines.extend([f"- {a.severity}: {a.message}" for a in top_alerts])
    if len(alerts) > len(top_alerts):
        lines.append(f"- ... and {len(alerts)-len(top_alerts)} more alerts")
    lines.extend(["", "Action", "------", action, ""])
    return subject, "\n".join(lines)


def send_email_alert(recipients_file: Path, alerts: list[Alert], rc: int, snapshot: Snapshot, env_label: str, service_name: str, top_n: int) -> None:
    recipients = load_recipients(recipients_file)
    subject, body = build_email_content(alerts, rc, snapshot, env_label, service_name, top_n)
    cmd = ["mail", "-s", subject, ",".join(recipients)]
    try:
        proc = subprocess.run(cmd, input=body + "\n", text=True, capture_output=True, check=False)
    except OSError as exc:
        print(f"WARNING: failed to execute mail command: {exc}")
        return
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        print(f"WARNING: email alert failed rc={proc.returncode}: {err}")


def print_result(alerts: list[Alert]) -> int:
    if not alerts:
        print("OK: no deviations detected")
        return OK
    rc = CRITICAL if any(a.severity == "CRITICAL" for a in alerts) else WARNING
    label = "CRITICAL" if rc == CRITICAL else "WARNING"
    print(f"{label}: " + " | ".join(f"{a.severity}: {a.message}" for a in alerts))
    return rc


def parse_args() -> argparse.Namespace:
    parser = CliParser(description="Simple MySQL InnoDB ClusterSet monitor")
    parser.add_argument("--mysqlsh-target", default=os.environ.get("MYSQLSH_TARGET"), help="mysqlsh target, e.g. BACKUSER@10.85.2.6")
    parser.add_argument("--mysqlsh-bin", default="mysqlsh", help="mysqlsh binary path")
    parser.add_argument("--mysqlsh-cmd", default="dba.getClusterSet().status({extended:1})", help="mysqlsh JS expression")
    parser.add_argument("--mysqlsh-password-env", default="MYSQLSH_PASSWORD", help="env var with mysqlsh password")
    parser.add_argument("--state-file", type=Path, default=Path("/home/umesh/last_status.json"), help="state file path")
    parser.add_argument("--sync-lag-threshold", type=float, default=5.0)
    parser.add_argument("--async-lag-threshold", type=float, default=30.0)
    parser.add_argument("--alert-email", action="store_true", help="enable email alerts")
    parser.add_argument("--critical-recipients-file", type=Path, default=Path("/home/umesh/critical_recipients.txt"))
    parser.add_argument("--warning-recipients-file", type=Path, default=Path("/home/umesh/warning_recipients.txt"))
    parser.add_argument("--env-label", default="PROD")
    parser.add_argument("--service-name", nargs="+", default=["Skilrock", "DBA", "Team"])
    parser.add_argument("--email-top-n", type=int, default=10)
    parser.add_argument("--version", action="version", version=f"%(prog)s {SCRIPT_VERSION}")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.mysqlsh_target:
        print("UNKNOWN: --mysqlsh-target (or env MYSQLSH_TARGET) is required")
        return UNKNOWN

    current_payload = fetch_current_status(
        mysqlsh_bin=args.mysqlsh_bin,
        mysqlsh_cmd=args.mysqlsh_cmd,
        mysqlsh_target=args.mysqlsh_target,
        mysqlsh_password=os.environ.get(args.mysqlsh_password_env),
    )
    current_snapshot = build_snapshot(current_payload)

    alerts = assess_snapshot(current_snapshot, args.sync_lag_threshold, args.async_lag_threshold)

    if args.state_file.exists():
        prev_snapshot = build_snapshot(load_json(args.state_file))
        alerts.extend(compare_snapshots(current_snapshot, prev_snapshot))

    args.state_file.write_text(json.dumps(current_payload, indent=2, sort_keys=True), encoding="utf-8")

    rc = print_result(alerts)

    service_name = " ".join(args.service_name)
    if alerts and args.alert_email:
        recipients = args.critical_recipients_file if rc == CRITICAL else args.warning_recipients_file
        send_email_alert(recipients, alerts, rc, current_snapshot, args.env_label, service_name, args.email_top_n)

    return rc


if __name__ == "__main__":
    sys.exit(main())
  
