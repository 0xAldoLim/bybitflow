"""Return unused worker allocations between cycles; referenced evidence stays live."""

import ctypes
import gc
import sys

import pyarrow as pa


def release_unused():
    result = dict(collected=gc.collect(), arrow_bytes=pa.total_allocated_bytes(), heap_trimmed=False)
    try:
        pa.default_memory_pool().release_unused()
        result["arrow_released"] = True
    except (NotImplementedError, RuntimeError) as exc:
        result["arrow_error"] = type(exc).__name__
    if sys.platform == "linux":
        try:
            trim = ctypes.CDLL(None).malloc_trim
            trim.argtypes, trim.restype = [ctypes.c_size_t], ctypes.c_int
            result["heap_trimmed"] = bool(trim(0))
        except (AttributeError, OSError):
            pass  # Non-glibc platforms keep their normal allocator behavior.
    return result
