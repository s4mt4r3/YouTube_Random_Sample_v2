"""Discover Slurm limits on the cluster this runs on (run on an Amarel login node).

Writes <out>/limits_raw.txt (full command output) and <out>/limits.json
(parsed fields used by tools/size_campaign.py). Nothing is hardcoded:
every number comes from sinfo/scontrol/sacctmgr, plus a real outbound
HTTPS test from a compute node via srun.
"""

from __future__ import annotations

import argparse
import getpass
import json
import re
import subprocess
from pathlib import Path


def run(cmd: str, timeout: int = 120) -> str:
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout, check=False)
        return (r.stdout + (("\n[stderr]\n" + r.stderr) if r.stderr.strip() else "")).strip()
    except subprocess.TimeoutExpired:
        return f"[timeout after {timeout}s]"


def parse_kv(line: str) -> dict:
    return dict(m.groups() for m in re.finditer(r"(\S+?)=(\S*)", line))


def tres_cpu(tres: str) -> int | None:
    m = re.search(r"(?:^|,)cpu=(\d+)", tres or "")
    return int(m.group(1)) if m else None


def smallest(values) -> int | None:
    vals = [v for v in values if v]
    return min(vals) if vals else None


def parse_minutes(s: str | None) -> int | None:
    """Slurm time ('UNLIMITED', '3-00:00:00', '72:00:00', '4320') -> minutes."""
    if not s or s.upper() in ("UNLIMITED", "INFINITE", "NONE", "N/A"):
        return None
    days = 0
    if "-" in s:
        d, s = s.split("-", 1)
        days = int(d)
    parts = [int(x) for x in s.split(":")]
    if len(parts) == 1:
        return days * 1440 + parts[0]
    while len(parts) < 3:
        parts.insert(0, 0)
    h, m, sec = parts
    return days * 1440 + h * 60 + m + (1 if sec else 0)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=Path("limits"))
    ap.add_argument("--partition", default="main")
    ap.add_argument("--no-srun", action="store_true", help="skip the compute-node internet test")
    a = ap.parse_args(argv)
    a.out.mkdir(parents=True, exist_ok=True)
    user = getpass.getuser()

    cmds = {
        "sinfo_summary": "sinfo -s",
        "sinfo_partitions": "sinfo -o '%P %a %l %D %c %m %G' ",
        "partitions": "scontrol show partition -o",
        "config": "scontrol show config",
        "qos": "sacctmgr -nP show qos format=Name,Priority,MaxWall,MaxTRESPU,MaxJobsPU,MaxSubmitPU,GrpTRES,MaxTRES,Flags",
        "assoc": f"sacctmgr -nP show assoc where user={user} format=Cluster,Account,Partition,QOS,DefaultQOS,MaxJobs,MaxSubmit,MaxWall,GrpTRES,MaxTRES,GrpJobs,GrpSubmit",
        "user_qos_default": f"sacctmgr -nP show user {user} withassoc format=User,DefaultAccount,QOS,DefaultQOS",
    }
    raw = {k: run(v) for k, v in cmds.items()}
    if not a.no_srun:
        raw["compute_internet"] = run(
            f"srun -p {a.partition} -N1 -n1 -t 00:03:00 --mem=500M bash -c "
            "'hostname; curl -sS -m 20 -o /dev/null -w \"youtube_http=%{http_code}\\n\" https://www.youtube.com/; "
            "curl -sS -m 20 -o /dev/null -w \"innertube_http=%{http_code}\\n\" -X POST "
            "-H \"Content-Type: application/json\" "
            "-d \"{\\\"context\\\":{\\\"client\\\":{\\\"clientName\\\":\\\"WEB\\\",\\\"clientVersion\\\":\\\"2.20240101.00.00\\\"}},\\\"query\\\":\\\"test\\\"}\" "
            "https://www.youtube.com/youtubei/v1/search'", timeout=600)

    with open(a.out / "limits_raw.txt", "w", encoding="utf-8") as fh:
        for k, v in raw.items():
            fh.write(f"===== {k}: {cmds.get(k, 'srun curl test')}\n{v}\n\n")

    cfg = {}
    for line in raw["config"].splitlines():
        if "=" in line:
            k, _, v = line.partition("=")
            cfg[k.strip()] = v.strip()
    parts = {}
    for line in raw["partitions"].splitlines():
        kv = parse_kv(line)
        if "PartitionName" in kv:
            parts[kv["PartitionName"]] = kv
    part = parts.get(a.partition, {})
    qos_rows = {}
    for line in raw["qos"].splitlines():
        f = line.split("|")
        if len(f) >= 9:
            qos_rows[f[0]] = dict(zip(["name", "priority", "max_wall", "max_tres_pu", "max_jobs_pu",
                                       "max_submit_pu", "grp_tres", "max_tres", "flags"], f))
    assoc_rows = [dict(zip(["cluster", "account", "partition", "qos", "default_qos", "max_jobs", "max_submit",
                            "max_wall", "grp_tres", "max_tres", "grp_jobs", "grp_submit"], l.split("|")))
                  for l in raw["assoc"].splitlines() if "|" in l]
    qos_name = part.get("QoS") if part.get("QoS") not in (None, "N/A") else None
    if not qos_name and assoc_rows:
        qos_name = assoc_rows[0].get("default_qos") or (assoc_rows[0].get("qos") or "").split(",")[0]
    q = qos_rows.get(qos_name or "", {})

    def to_int(x):
        try:
            return int(x)
        except (TypeError, ValueError):
            return None

    limits = {
        "user": user,
        "partition": a.partition,
        "partitions_available": sorted(parts),
        "max_array_size": to_int(cfg.get("MaxArraySize")),
        "max_job_count": to_int(cfg.get("MaxJobCount")),
        "partition_max_time_min": parse_minutes(part.get("MaxTime")),
        "partition_default_time_min": parse_minutes(part.get("DefaultTime")),
        "partition_total_cpus": to_int(part.get("TotalCPUs")),
        "partition_nodes": to_int(part.get("TotalNodes")),
        "partition_max_cpus_per_user": tres_cpu(part.get("MaxCPUsPerUser", "")) or to_int(part.get("MaxCPUsPerUser")),
        "preempt_mode": part.get("PreemptMode"),
        "qos": qos_name,
        "qos_max_wall_min": parse_minutes(q.get("max_wall")),
        "qos_max_jobs_pu": to_int(q.get("max_jobs_pu")),
        "qos_max_submit_pu": to_int(q.get("max_submit_pu")),
        "qos_max_cpus_pu": tres_cpu(q.get("max_tres_pu", "")),
        "assoc_max_jobs": smallest(to_int(r.get("max_jobs")) for r in assoc_rows),
        "assoc_max_submit": smallest(to_int(r.get("max_submit")) for r in assoc_rows),
        "assoc_grp_cpus": smallest(tres_cpu(r.get("grp_tres", "")) for r in assoc_rows),
        "compute_internet": raw.get("compute_internet", "not tested"),
    }
    (a.out / "limits.json").write_text(json.dumps(limits, indent=1))
    print(json.dumps(limits, indent=1))
    print(f"\nraw output: {a.out / 'limits_raw.txt'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
