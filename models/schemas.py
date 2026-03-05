"""
models/schemas.py
Dataclasses and enums for job/queue management.
"""
import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Union

from fastapi import HTTPException


class JobStatus(str, Enum):
    PENDING    = "pending"
    PROCESSING = "processing"
    COMPLETED  = "completed"
    FAILED     = "failed"
    CANCELLED  = "cancelled"


@dataclass
class ReportJob:
    job_id: str
    payload: Dict[str, Any]
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Union[str, Dict[str, Any]]] = None
    error: Optional[str] = None
    multi_report: bool = False
    structured: bool = False
    employee_report: bool = False


class ReportQueue:
    def __init__(self, max_size: int = 100):
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=max_size)
        self.jobs: Dict[str, ReportJob] = {}
        self.job_counter = 0
        self._lock = asyncio.Lock()

    async def add_job(
        self,
        payload: Dict[str, Any],
        multi_report: bool = False,
        structured: bool = False,
        employee_report: bool = False,
    ) -> str:
        async with self._lock:
            self.job_counter += 1
            job_id = f"job_{int(time.time())}_{self.job_counter}"
            job = ReportJob(
                job_id=job_id,
                payload=payload,
                multi_report=multi_report,
                structured=structured,
                employee_report=employee_report,
            )
            self.jobs[job_id] = job
            try:
                await self.queue.put(job)
                return job_id
            except asyncio.QueueFull:
                job.status = JobStatus.FAILED
                job.error = "Queue is full. Please try again later."
                raise HTTPException(status_code=503, detail=job.error)

    async def get_job(self) -> ReportJob:
        return await self.queue.get()

    def get_queue_stats(self) -> Dict[str, Any]:
        statuses: Dict[str, int] = {}
        for job in self.jobs.values():
            statuses[job.status.value] = statuses.get(job.status.value, 0) + 1
        return {
            "queue_size": self.queue.qsize(),
            "total_jobs": len(self.jobs),
            "status_breakdown": statuses,
        }
