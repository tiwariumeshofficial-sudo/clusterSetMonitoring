# ClusterSet Monitoring (Simple)

Simple monitor for MySQL InnoDB ClusterSet.

## What this simplified version keeps

- Fetch current status from `mysqlsh`
- Detect health/replication/member deviations
- Compare with previous run using `--state-file`
- Send clean email alerts with severity-based recipients


## Main command

```bash
python3 monitor_clusterset.py \
  --mysqlsh-target 'BACKUSER@10.85.2.6' \
  --state-file /home/umesh/last_status.json \
  --alert-email \
  --critical-recipients-file /home/umesh/critical_recipients.txt \
  --warning-recipients-file /home/umesh/warning_recipients.txt \
  --env-label PROD \
  --service-name Skilrock DBA Team
```

Compatibility name also works:

```bash
python3 monitor_clusterset1.py --mysqlsh-target 'MonitorUer@HostIP'
```

## Recipients file format

```text
email1@gmail.com,email2@gmail.com
```

## Email subject

- CRITICAL: `[MySQL ClusterSet] <clusterSetName/domainName> - ACTION REQUIRED`
- WARNING: `[MySQL ClusterSet] <clusterSetName/domainName>`

## Exit codes

- `0` = OK
- `1` = WARNING
- `2` = CRITICAL
- `3` = UNKNOWN
