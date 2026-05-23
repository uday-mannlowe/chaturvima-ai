import argparse
import asyncio
import os
import time
from typing import Any, Dict, List, Optional

import httpx
from dotenv import load_dotenv


def _split_csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


async def _get_health(client: httpx.AsyncClient, base_url: str) -> Dict[str, Any]:
    response = await client.get(f"{base_url}/health")
    response.raise_for_status()
    return response.json()


async def _submit_one(
    client: httpx.AsyncClient,
    base_url: str,
    endpoint: str,
    employee: str,
    index: int,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "employee": employee,
        "force_regenerate": args.force,
    }
    if args.cycle_name:
        payload["cycle_name"] = args.cycle_name
    if args.submission_id:
        payload["submission_id"] = args.submission_id
    if args.send_frappe_creds:
        frappe_key = os.getenv("FRAPPE_API_KEY", "").strip()
        frappe_secret = os.getenv("FRAPPE_API_SECRET", "").strip()
        if frappe_key and frappe_secret:
            payload["frappe_api_key"] = frappe_key
            payload["frappe_api_secret"] = frappe_secret

    started = time.perf_counter()
    try:
        response = await client.post(f"{base_url}{endpoint}", json=payload)
        elapsed = time.perf_counter() - started
        try:
            body: Any = response.json()
        except ValueError:
            body = response.text[:500]
        return {
            "index": index,
            "employee": employee,
            "status_code": response.status_code,
            "elapsed": elapsed,
            "body": body,
        }
    except Exception as exc:
        elapsed = time.perf_counter() - started
        return {
            "index": index,
            "employee": employee,
            "status_code": None,
            "elapsed": elapsed,
            "body": str(exc),
        }


async def _bounded_submit(
    sem: asyncio.Semaphore,
    client: httpx.AsyncClient,
    base_url: str,
    endpoint: str,
    employee: str,
    index: int,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    async with sem:
        return await _submit_one(client, base_url, endpoint, employee, index, args)


def _queue_stats(health: Dict[str, Any]) -> Dict[str, Any]:
    return health.get("queue", {}) if isinstance(health, dict) else {}


async def run(args: argparse.Namespace) -> int:
    load_dotenv(override=True)

    base_url = args.base_url.rstrip("/")
    endpoint = args.endpoint if args.endpoint.startswith("/") else f"/{args.endpoint}"
    employees = _split_csv(args.employees) if args.employees else [args.employee]
    employees = (employees * ((args.repeat + len(employees) - 1) // len(employees)))[: args.repeat]

    headers: Dict[str, str] = {}
    auth = args.authorization or os.getenv("AUTHORIZATION", "").strip()
    if auth:
        headers["Authorization"] = auth

    timeout = httpx.Timeout(args.timeout)
    limits = httpx.Limits(max_connections=max(args.concurrency, 10), max_keepalive_connections=args.concurrency)

    async with httpx.AsyncClient(headers=headers, timeout=timeout, limits=limits) as client:
        try:
            baseline_health = await _get_health(client, base_url)
        except Exception as exc:
            print(f"Health check failed for {base_url}/health: {exc}")
            return 2

        baseline_queue = _queue_stats(baseline_health)
        baseline_statuses = baseline_queue.get("status_breakdown", {}) or {}
        baseline_done = int(baseline_statuses.get("completed", 0)) + int(baseline_statuses.get("failed", 0))

        print(f"Base URL: {base_url}")
        print(f"Endpoint: {endpoint}")
        print(f"Requests: {len(employees)}")
        print(f"Concurrency: {args.concurrency}")
        print(f"Queue before: {baseline_queue}")
        print()

        sem = asyncio.Semaphore(args.concurrency)
        started = time.perf_counter()
        tasks = [
            _bounded_submit(sem, client, base_url, endpoint, employee, index, args)
            for index, employee in enumerate(employees, start=1)
        ]
        results = await asyncio.gather(*tasks)
        submit_elapsed = time.perf_counter() - started

        print("Submit results:")
        ok_count = 0
        submitted_jobs = 0
        for result in results:
            body = result["body"]
            job_id: Optional[str] = body.get("job_id") if isinstance(body, dict) else None
            status = body.get("status") if isinstance(body, dict) else None
            if result["status_code"] and 200 <= int(result["status_code"]) < 300:
                ok_count += 1
            if job_id:
                submitted_jobs += 1
            print(
                f"  #{result['index']:02d} {result['employee']}: "
                f"HTTP {result['status_code']} in {result['elapsed']:.2f}s "
                f"status={status} job_id={job_id}"
            )

        print()
        print(f"Submitted {ok_count}/{len(results)} requests in {submit_elapsed:.2f}s.")
        print(f"Background jobs accepted: {submitted_jobs}")

        if not args.watch or submitted_jobs == 0:
            return 0 if ok_count == len(results) else 1

        deadline = time.monotonic() + args.watch_seconds
        print()
        print("Watching /health. Stop with Ctrl+C.")
        while time.monotonic() < deadline:
            health = await _get_health(client, base_url)
            queue = _queue_stats(health)
            statuses = queue.get("status_breakdown", {}) or {}
            done_now = int(statuses.get("completed", 0)) + int(statuses.get("failed", 0))
            done_delta = max(done_now - baseline_done, 0)
            print(
                f"  queue={queue.get('queue_size')} "
                f"available={queue.get('queue_available')} "
                f"statuses={statuses} "
                f"done_for_this_run~={done_delta}/{submitted_jobs}"
            )
            if done_delta >= submitted_jobs:
                break
            await asyncio.sleep(args.poll_seconds)

    return 0 if ok_count == len(results) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Submit report-generation requests concurrently.")
    parser.add_argument("--base-url", default=os.getenv("REPORT_API_BASE_URL", "http://localhost:5000"))
    parser.add_argument("--endpoint", default="/generate-employee-report")
    parser.add_argument("--employee", default="HR-EMP-00031")
    parser.add_argument("--employees", default="", help="Comma-separated employee IDs. Overrides --employee.")
    parser.add_argument("--repeat", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--cycle-name", default="")
    parser.add_argument("--submission-id", default="")
    parser.add_argument("--force", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--send-frappe-creds", action="store_true")
    parser.add_argument("--authorization", default="")
    parser.add_argument("--timeout", type=float, default=90.0)
    parser.add_argument("--watch", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--watch-seconds", type=int, default=1800)
    parser.add_argument("--poll-seconds", type=float, default=10.0)
    args = parser.parse_args()

    if args.repeat < 1:
        parser.error("--repeat must be at least 1")
    if args.concurrency < 1:
        parser.error("--concurrency must be at least 1")

    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
