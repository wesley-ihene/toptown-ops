"""HR specialist agent package."""

from apps.hr_agent.worker import HrAgentWorker, process_work_item

__all__ = ["HrAgentWorker", "process_work_item"]
