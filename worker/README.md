# ParsYa WSL worker

The worker makes outbound HTTPS requests to the control plane. Do **not** open an inbound firewall port in WSL and do not expose the WSL Flask/FastAPI development server.

1. Install dependencies in the existing ParsYa venv:
   `pip install -r /home/dima/ParsYa/parser-worker/requirements.txt`
2. Add the environment variables from the root README to `/home/dima/ParsYa/.env`.
3. Make the existing main parser accept remote job parameters:
   `python /home/dima/ParsYa/parser-worker/patch_parser_env.py /home/dima/ParsYa/ParsYa_main_with_tracking.py`
   The patch creates a .bak backup first and is safe to run again.
4. Start the agent:
   `/home/dima/ParsYa/.venv/bin/python /home/dima/ParsYa/parser-worker/agent.py`

The agent only launches the configured Python parser script. The web panel does not send arbitrary shell commands. It sends the job type, city and keyword phrase only.
