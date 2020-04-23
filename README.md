# Prototype Code to Analyze Merritt Log Files

- For each storage server
  - find the logs directory for the each storage service
  - scp content to local drive ~/work/logs/storeXX
    - scp user@host:/.../logs/localhost_access_logs.2020_xx ~/work/logs/host
python3 log-analyzer.py
