"""Audit logging helper for bronze job execution."""

from datetime import datetime


class AuditLogger:
    """Store job execution metadata for bronze load runs."""

    def __init__(self, job_name: str):
        self.job_name = job_name

    def log_run(self, table_name: str, start_time: datetime, end_time: datetime, status: str, records: int = 0):
        """Capture execution metadata for audit and reconciliation."""
        return {
            "job_name": self.job_name,
            "table_name": table_name,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "status": status,
            "records": records,
        }
