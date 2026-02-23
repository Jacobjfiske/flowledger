import time
from collections.abc import Callable


class RetryExhaustedError(RuntimeError):
    pass


def run_with_retries(
    fn: Callable[[], object],
    *,
    max_retries: int,
    backoff_seconds: float,
    on_attempt_failure: Callable[[int, Exception], None] | None = None,
    should_retry: Callable[[Exception], bool] | None = None,
) -> object:
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 2):
        try:
            return fn()
        except Exception as exc:
            last_error = exc
            if on_attempt_failure:
                on_attempt_failure(attempt, exc)

            retry_allowed = True if should_retry is None else should_retry(exc)
            if attempt > max_retries or not retry_allowed:
                break
            time.sleep(backoff_seconds * attempt)

    raise RetryExhaustedError(str(last_error)) from last_error
