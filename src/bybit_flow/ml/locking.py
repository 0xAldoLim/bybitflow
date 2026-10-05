"""Small process locks keep bounded manual work from racing the background worker."""

import os
from contextlib import contextmanager


@contextmanager
def exclusive(path):
    with path.open("a+") as stream:
        if os.name == "nt":
            import msvcrt

            stream.write("0")
            stream.flush()
            stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
