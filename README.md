# ParsYa Control Plane

Web dashboard for managing ParsYa workers running on Windows Subsystem for Linux (WSL). The WSL worker makes outbound HTTPS requests to this server and polls for jobs; no inbound access to WSL is required.

## Components

- FastAPI web application for dashboard, job queue, creative gallery and worker status.
- SQLite by default for a simple first deployment; can be switched to PostgreSQL later.
- Python worker agent intended to run in WSL and execute local ParsYa parsing jobs.
- Nginx reverse proxy with TLS in front of the app.

## Security

Keep `PARSYA_ADMIN_PASSWORD`, `PARSYA_WORKER_TOKEN`, and `PARSYA_SECRET_KEY` in a server-side `.env` file. Never commit real secrets. The worker only makes outbound HTTPS requests; do not expose an HTTP port on WSL.
