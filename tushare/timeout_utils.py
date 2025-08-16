import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout


def get_timeout_seconds() -> int:
    try:
        return int(os.environ.get("TS_TIMEOUT_SEC", "60"))
    except Exception:
        return 60


def call_with_timeout(func, timeout_sec: int, *args, **kwargs):
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout_sec)
        except FuturesTimeout as e:
            raise TimeoutError(f"function call timed out after {timeout_sec}s") from e


def pro_call_with_timeout(pro, method_name: str, timeout_sec: int, **kwargs):
    method = getattr(pro, method_name)
    return call_with_timeout(method, timeout_sec, **kwargs)


