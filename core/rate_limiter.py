"""
core/rate_limiter.py
Token-bucket rate limiter for Groq API calls.
"""
import asyncio
import time


class RateLimiter:
    def __init__(self, rate_per_minute: int):
        self.rate_per_minute = rate_per_minute
        self.tokens = rate_per_minute
        self.last_update = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(
                self.rate_per_minute,
                self.tokens + elapsed * (self.rate_per_minute / 60),
            )
            self.last_update = now

            if self.tokens < 1:
                wait_time = (1 - self.tokens) / (self.rate_per_minute / 60)
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1
