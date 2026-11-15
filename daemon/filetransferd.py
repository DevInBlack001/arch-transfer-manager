#!/usr/bin/env python3
"""filetransferd: a per-user daemon that runs queued file copy/move jobs
with rsync in its own process tree.

The point of running transfers here instead of inside a file manager is
survivability: a GTK file manager's built-in copy runs as threads inside
its own process, so a crash there kills the transfer too. This daemon is
a separate long-lived process (restarted by systemd on failure, queue
persisted to disk) so pause/resume/cancel/reorder keep working, and an
in-flight transfer keeps running, no matter what happens to whichever
file manager or Quickshell panel queued it.

Wire protocol: newline-delimited JSON over a Unix socket restricted to
the owning user (runtime-dir permissions plus an SO_PEERCRED check).
Never trust path input as shell text; rsync is always invoked with an
explicit argv list and a `--` separator before user-supplied paths.
"""
import asyncio
import contextlib
import json
import logging
import os
import re
import signal
import stat
import sys
import tempfile
import time
import uuid
import zipfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import archive
import ft_common as common

ARCHIVE_MODES = ("compress", "extract")
JOB_MODES = ("copy", "move") + ARCHIVE_MODES

MAX_CONCURRENT = max(1, min(4, int(os.environ.get("FILETRANSFERD_MAX_CONCURRENT", "1") or "1")))
MAX_HISTORY = 200
GRACE_KILL_SEC = 5.0

PROGRESS_RE = re.compile(r"^\s*([\d,]+)\s+(\d+)%\s+([\d.]+)(\w+)/s\s+(\d+:\d+:\d+|\d+:\d+)")
SUMMARY_PREFIXES = ("sent ", "total size is", "speedup is", "building file list")


def _parse_size_rate(value, unit):
    try:
        num = float(value)
    except ValueError:
        return 0.0
    mult = {"b": 1.0, "k": 1024.0, "m": 1024.0 ** 2, "g": 1024.0 ** 3, "t": 1024.0 ** 4}.get(unit[:1].lower(), 1.0)
    return num * mult


def _parse_eta(text):
    try:
        parts = [int(p) for p in text.split(":")]
    except ValueError:
        return -1
    total = 0
    for p in parts:
        total = total * 60 + p
    return total


class JobStartError(Exception):
    def __init__(self, tool, cause):
        super().__init__("failed to start %s: %s" % (tool, cause))
        self.tool = tool
        self.cause = cause


class Job:
    def __init__(self, mode, sources, dest):
        now = time.time()
        self.id = uuid.uuid4().hex
        self.mode = mode
        self.sources = sources
        self.dest = dest
        self.state = "queued"
        self.bytes_done = 0
        self.bytes_total = -1
        self.speed_bps = 0.0
        self.eta_sec = -1
        self.current_file = ""
        self.error = ""
        self.created_at = now
        self.started_at = None
        self.finished_at = None

    def to_dict(self):
        return {
            "id": self.id, "mode": self.mode, "sources": self.sources, "dest": self.dest,
            "state": self.state, "bytesDone": self.bytes_done, "bytesTotal": self.bytes_total,
            "speedBps": self.speed_bps, "etaSec": self.eta_sec, "currentFile": self.current_file,
            "error": self.error, "createdAt": self.created_at, "startedAt": self.started_at,
            "finishedAt": self.finished_at,
        }

    @classmethod
    def from_dict(cls, d):
        # queue.json is trusted-ish (this daemon wrote it), but it's still a
        # file on disk that could be hand-edited, truncated, or corrupted;
        # checking shapes here means a bad entry is skipped by the caller
        # instead of producing a job with the wrong types deep in the queue.
        if not isinstance(d, dict):
            raise TypeError("job entry is not an object")
        job_id = d["id"]
        mode = d["mode"]
        sources = d["sources"]
        dest = d["dest"]
        state = d["state"]
        if not isinstance(job_id, str) or not job_id:
            raise TypeError("job id must be a non-empty string")
        if mode not in JOB_MODES:
            raise TypeError("job mode is not recognized")
        if not isinstance(sources, list) or not all(isinstance(s, str) for s in sources):
            raise TypeError("job sources must be a list of strings")
        if not isinstance(dest, str):
            raise TypeError("job dest must be a string")
        if state not in ("queued", "running", "paused", "done", "error", "cancelled"):
            raise TypeError("job state is not recognized")

        job = cls.__new__(cls)
        job.id = job_id
        job.mode = mode
        job.sources = sources
        job.dest = dest
        job.state = state
        job.bytes_done = d.get("bytesDone", 0)
        job.bytes_total = d.get("bytesTotal", -1)
        job.speed_bps = d.get("speedBps", 0.0)
        job.eta_sec = d.get("etaSec", -1)
        job.current_file = d.get("currentFile", "")
        job.error = d.get("error", "")
        job.created_at = d.get("createdAt", time.time())
        job.started_at = d.get("startedAt")
        job.finished_at = d.get("finishedAt")
        return job


class TransferDaemon:
    def __init__(self):
        self.jobs = {}
        self.queue_order = []
        self.running_ids = set()
        self.procs = {}
        self.pgids = {}
        self.lock = asyncio.Lock()
        self._save_lock = asyncio.Lock()
        self.log = logging.getLogger("filetransferd")

    # ---------------------------------------------------------- persistence

    def _load(self):
        path = common.queue_file()
        if not os.path.lexists(path):
            return
        fd = None
        try:
            # O_NOFOLLOW plus an explicit regular-file check: this path is
            # normally ours (we wrote it via mkstemp + rename), but another
            # process running as this same user could still have replaced it
            # with a symlink before this open. Refuse to follow it rather
            # than read whatever it points at.
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode):
                self.log.warning("queue state path is not a regular file, refusing to load")
                return
            with os.fdopen(fd, "rb") as fh:
                fd = None  # fdopen owns the descriptor now
                # Bounded read: json.load() on an unbounded stream will
                # happily try to buffer however much is there. Read one byte
                # past the cap so an oversized file is detected and rejected
                # rather than truncated into invalid JSON and misread.
                raw_bytes = fh.read(common.MAX_QUEUE_STATE_BYTES + 1)
            if len(raw_bytes) > common.MAX_QUEUE_STATE_BYTES:
                self.log.warning("queue state file exceeds %d bytes, refusing to load",
                                  common.MAX_QUEUE_STATE_BYTES)
                return
            raw = json.loads(raw_bytes.decode("utf-8"))
        except (OSError, ValueError) as exc:
            self.log.warning("could not read queue state: %s", exc)
            return
        finally:
            if fd is not None:
                os.close(fd)
        if not isinstance(raw, dict) or not isinstance(raw.get("jobs"), list):
            self.log.warning("queue state file has an unexpected shape, ignoring it")
            return
        for entry in raw.get("jobs", []):
            try:
                job = Job.from_dict(entry)
            except (KeyError, TypeError):
                continue
            # Any process that was mid-transfer died with the previous daemon
            # instance. rsync's --partial-dir means restarting is safe and
            # resumes from where it left off, so put it back in line.
            if job.state in ("running", "paused"):
                job.state = "queued"
                job.current_file = ""
            self.jobs[job.id] = job
        for job in sorted(self.jobs.values(), key=lambda j: j.created_at):
            if job.state == "queued":
                self.queue_order.append(job.id)

    def _write_snapshot(self, data):
        path = common.queue_file()
        directory = os.path.dirname(path)
        tmp = None
        try:
            # A fixed "path.tmp" name is predictable: another process in
            # this account could pre-create it as a symlink, and O_CREAT
            # without O_EXCL would follow that symlink and truncate
            # whatever it points at. mkstemp() picks an unpredictable name
            # and opens it O_EXCL itself, closing that window; it also
            # creates the file 0600, so no separate chmod is needed.
            fd, tmp = tempfile.mkstemp(prefix=".queue.", suffix=".tmp", dir=directory)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(data, fh)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
            tmp = None
        except OSError as exc:
            self.log.warning("could not persist queue state: %s", exc)
        finally:
            if tmp is not None:
                with contextlib.suppress(OSError):
                    os.unlink(tmp)

    def _snapshot(self):
        # Build the plain-dict snapshot on the event loop thread, before
        # handing it to a worker thread. self.jobs can be mutated the
        # instant control returns here, and iterating a dict on one thread
        # while another mutates it raises RuntimeError.
        return {"jobs": [j.to_dict() for j in self.jobs.values()]}

    async def _persist(self, data):
        # fsync can stall for seconds under heavy concurrent disk writes (an
        # active rsync job on the same filesystem, notably). ext4's ordered
        # mode in particular can make fsync of this small file wait on an
        # unrelated in-flight transfer's dirty pages. Serialize writes here
        # (not in the caller) so overlapping saves queue up instead of
        # racing on the same tmp path, and run each one in a thread so it
        # never blocks the event loop.
        async with self._save_lock:
            await asyncio.get_running_loop().run_in_executor(None, self._write_snapshot, data)

    async def _save(self):
        """Blocking save: only for paths where the caller must know the
        write finished before proceeding, i.e. daemon shutdown."""
        await self._persist(self._snapshot())

    def _save_nowait(self):
        """Fire-and-forget save for the hot control paths (pause/resume/
        cancel/...). The mutation and any signal are already applied by the
        time this is called, so the client reply shouldn't wait on disk
        latency too. Worst case on a crash before this lands: the affected
        job reloads as "queued" next start, per _load()'s recovery rule --
        never data loss, since rsync's own --partial-dir owns file safety."""
        asyncio.ensure_future(self._persist(self._snapshot()))

    def _trim_history(self):
        finished = [j for j in self.jobs.values() if j.state in ("done", "error", "cancelled")]
        finished.sort(key=lambda j: j.finished_at or 0)
        excess = len(finished) - MAX_HISTORY
        for job in finished[:max(0, excess)]:
            del self.jobs[job.id]

    # ---------------------------------------------------------- validation

    def _validate_archive(self, mode, sources, dest):
        """compress: sources are siblings in one directory and dest is the new
        archive path. extract: one archive source and dest is the target folder."""
        for src in sources:
            if not isinstance(src, str) or not os.path.isabs(src) or "\x00" in src:
                return "archive jobs need absolute local paths"
        if "\x00" in dest or not os.path.isabs(dest):
            return "archive jobs need an absolute destination"
        if mode == "compress":
            if archive.archive_kind(dest) is None:
                return "use .zip, .tar, .tar.gz, .tgz, .tar.xz, .tar.zst or .tar.bz2"
            parents = {os.path.dirname(s.rstrip("/")) for s in sources}
            if len(parents) != 1:
                return "items to compress must be in the same folder"
            for src in sources:
                if not os.path.lexists(src):
                    return "source does not exist: " + src
            if os.path.lexists(dest):
                return "'%s' already exists" % os.path.basename(dest)
            if not os.path.isdir(os.path.dirname(dest)):
                return "destination folder does not exist"
            return None
        if len(sources) != 1:
            return "extract takes exactly one archive"
        src = sources[0]
        if not os.path.isfile(src):
            return "archive does not exist: " + src
        if archive.archive_kind(src) is None:
            return "unsupported archive type"
        if os.path.lexists(dest) and not os.path.isdir(dest):
            return "destination exists and is not a folder"
        if not os.path.isdir(os.path.dirname(dest.rstrip("/")) or "/"):
            return "destination parent folder does not exist"
        return None

    def _validate_enqueue(self, mode, sources, dest):
        if mode not in JOB_MODES:
            return "mode must be one of: " + ", ".join(JOB_MODES)
        if not isinstance(sources, list) or not sources:
            return "sources must be a non-empty list"
        if len(sources) > common.MAX_SOURCES:
            return "too many sources"
        if not isinstance(dest, str) or not dest:
            return "dest must be a non-empty path"
        active = len(self.queue_order) + len(self.running_ids)
        active += sum(1 for j in self.jobs.values() if j.state == "paused")
        if active >= common.MAX_QUEUED_JOBS:
            return "queue is full"
        if mode in ARCHIVE_MODES:
            return self._validate_archive(mode, sources, dest)
        dest_remote = isinstance(dest, str) and common.is_remote_spec(dest)
        sources_remote = []
        for src in sources:
            if not isinstance(src, str) or not src or "\x00" in src:
                return "invalid source path"
            if common.is_remote_spec(src):
                sources_remote.append(True)
                continue
            sources_remote.append(False)
            if not os.path.lexists(src):
                return "source does not exist: " + src
        if all(sources_remote) and dest_remote:
            return "remote-to-remote transfers are not supported: one side must be local"
        if "\x00" in dest:
            return "invalid destination path"
        if not dest_remote and not os.path.isdir(os.path.realpath(dest)):
            return "destination is not a directory: " + dest
        return None

    # ---------------------------------------------------------- commands

    async def handle(self, cmd, args):
        args = args or {}
        if cmd == "ping":
            return {"ok": True, "pid": os.getpid()}
        if cmd == "list":
            return {"ok": True, "jobs": [j.to_dict() for j in self._ordered_jobs()],
                     "maxConcurrent": MAX_CONCURRENT}
        if cmd == "enqueue":
            return await self._enqueue(args)
        if cmd == "pause":
            return await self._pause(str(args.get("id", "")))
        if cmd == "resume":
            return await self._resume(str(args.get("id", "")))
        if cmd == "cancel":
            return await self._cancel(str(args.get("id", "")))
        if cmd == "reorder":
            return await self._reorder(str(args.get("id", "")), args.get("position"))
        if cmd == "clear_finished":
            async with self.lock:
                for jid in [j.id for j in self.jobs.values() if j.state in ("done", "error", "cancelled")]:
                    del self.jobs[jid]
                self._save_nowait()
            return {"ok": True}
        return {"ok": False, "error": "unknown command"}

    def _ordered_jobs(self):
        running = [self.jobs[i] for i in self.running_ids if i in self.jobs]
        queued = [self.jobs[i] for i in self.queue_order if i in self.jobs]
        paused = [j for j in self.jobs.values() if j.state == "paused"]
        history = sorted(
            (j for j in self.jobs.values() if j.state in ("done", "error", "cancelled")),
            key=lambda j: j.finished_at or 0, reverse=True,
        )
        return running + queued + paused + history

    async def _enqueue(self, args):
        mode = args.get("mode", "copy")
        sources = args.get("sources")
        dest = args.get("dest")
        async with self.lock:
            error = self._validate_enqueue(mode, sources, dest)
            if error:
                return {"ok": False, "error": error}
            if mode in ARCHIVE_MODES:
                resolved_dest = dest.rstrip("/") or "/"
            else:
                resolved_dest = dest if common.is_remote_spec(dest) else os.path.realpath(dest)
            job = Job(mode, list(sources), resolved_dest)
            self.jobs[job.id] = job
            self.queue_order.append(job.id)
            # Unlike pause/resume/cancel this isn't racing a huge transfer's
            # own I/O for responsiveness, and silently losing a just-queued
            # job to a crash is worse than the fire-and-forget jobs' worst
            # case (a state flag reverting to "queued"), so wait for it.
            await self._save()
        await self._schedule()
        return {"ok": True, "id": job.id}

    async def _pause(self, job_id):
        async with self.lock:
            job = self.jobs.get(job_id)
            if not job or job.state != "running":
                return {"ok": False, "error": "job is not running"}
            pgid = self.pgids.get(job_id)
            if pgid is not None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(pgid, signal.SIGSTOP)
            job.state = "paused"
            self.running_ids.discard(job_id)
            self._save_nowait()
        await self._schedule()
        return {"ok": True}

    async def _resume(self, job_id):
        async with self.lock:
            job = self.jobs.get(job_id)
            if not job or job.state != "paused":
                return {"ok": False, "error": "job is not paused"}
            if len(self.running_ids) < MAX_CONCURRENT:
                pgid = self.pgids.get(job_id)
                if pgid is not None:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(pgid, signal.SIGCONT)
                job.state = "running"
                self.running_ids.add(job_id)
            else:
                # No free slot: rejoin the queue rather than exceed the
                # concurrency limit. If its rsync process is still parked
                # (merely SIGSTOPped), _schedule() will just SIGCONT it
                # later instead of starting a fresh one.
                job.state = "queued"
                self.queue_order.insert(0, job_id)
            self._save_nowait()
        await self._schedule()
        return {"ok": True}

    async def _cancel(self, job_id):
        async with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return {"ok": False, "error": "unknown job"}
            if job.state in ("done", "error", "cancelled"):
                return {"ok": False, "error": "job already finished"}
            if job_id in self.queue_order:
                self.queue_order.remove(job_id)
            self.running_ids.discard(job_id)
            pgid = self.pgids.get(job_id)
            job.state = "cancelled"
            job.finished_at = time.time()
            self._trim_history()
            self._save_nowait()
        if pgid is not None:
            proc = self.procs.get(job_id)
            # A signal other than SIGCONT/SIGKILL is only delivered to a
            # stopped process once it resumes, so wake it before terminating.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(pgid, signal.SIGCONT)
            with contextlib.suppress(ProcessLookupError):
                os.killpg(pgid, signal.SIGTERM)
            if proc is not None:
                try:
                    await asyncio.wait_for(proc.wait(), timeout=GRACE_KILL_SEC)
                except asyncio.TimeoutError:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(pgid, signal.SIGKILL)
        await self._schedule()
        return {"ok": True}

    async def _reorder(self, job_id, position):
        if not isinstance(position, int):
            return {"ok": False, "error": "position must be an integer"}
        async with self.lock:
            if job_id not in self.queue_order:
                return {"ok": False, "error": "job is not queued"}
            self.queue_order.remove(job_id)
            position = max(0, min(position, len(self.queue_order)))
            self.queue_order.insert(position, job_id)
            self._save_nowait()
        return {"ok": True}

    # ---------------------------------------------------------- scheduling

    async def _schedule(self):
        async with self.lock:
            to_start_fresh = []
            to_resume_parked = []
            while len(self.running_ids) < MAX_CONCURRENT and self.queue_order:
                job_id = self.queue_order.pop(0)
                job = self.jobs.get(job_id)
                if not job:
                    continue
                missing = [s for s in job.sources
                           if not common.is_remote_spec(s) and not os.path.lexists(s)]
                if missing:
                    job.state = "error"
                    job.error = "source no longer exists: " + missing[0]
                    job.finished_at = time.time()
                    self._trim_history()
                    continue
                job.state = "running"
                if job.started_at is None:
                    job.started_at = time.time()
                self.running_ids.add(job_id)
                if job_id in self.procs:
                    to_resume_parked.append(job_id)
                else:
                    to_start_fresh.append(job_id)
            self._save_nowait()
        for job_id in to_resume_parked:
            pgid = self.pgids.get(job_id)
            if pgid is not None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(pgid, signal.SIGCONT)
        for job_id in to_start_fresh:
            asyncio.ensure_future(self._run_job(job_id))

    async def _run_job(self, job_id):
        job = self.jobs.get(job_id)
        if not job:
            return
        try:
            if job.mode in ARCHIVE_MODES:
                returncode, stderr_tail = await self._run_archive(job_id, job)
            else:
                returncode, stderr_tail = await self._run_rsync(job_id, job)
        except JobStartError as exc:
            async with self.lock:
                job.state = "error"
                job.error = "failed to start %s: %s" % (exc.tool, exc.cause)
                job.finished_at = time.time()
                self.running_ids.discard(job_id)
                self._trim_history()
                self._save_nowait()
            await self._schedule()
            return
        await self._finish_job(job_id, job, returncode, stderr_tail)

    async def _run_rsync(self, job_id, job):
        args = ["rsync", "-a", "--info=progress2,name1", "--no-inc-recursive",
                "--partial", "--partial-dir=.rsync-partial"]
        is_remote_job = common.is_remote_spec(job.dest) or any(common.is_remote_spec(s) for s in job.sources)
        if is_remote_job:
            # No BatchMode=yes: this subprocess has no controlling terminal
            # (start_new_session=True, no PTY), so OpenSSH already routes
            # any prompt (password, host-key confirmation) to $SSH_ASKPASS
            # instead of a TTY, as long as the daemon's own environment has
            # DISPLAY/WAYLAND_DISPLAY/SSH_ASKPASS set (see install.sh).
            # Auth and host trust stay entirely the system's own SSH's
            # responsibility; this daemon never touches keys or passwords.
            args.extend(["-e", "ssh -o ConnectTimeout=10"])
        if job.mode == "move":
            args.append("--remove-source-files")
        args.append("--")
        args.extend(job.sources)
        args.append(job.dest.rstrip("/") + "/")

        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        except OSError as exc:
            raise JobStartError("rsync", exc)

        self.procs[job_id] = proc
        self.pgids[job_id] = os.getpgid(proc.pid)
        stderr_tail = []

        async def read_stdout():
            # rsync's --info=progress2 rewrites a single terminal line with
            # \r between updates and only emits a real \n at true line
            # breaks (filenames, the final summary). readline() splits on
            # \n only, so it would sit blocked and hand back the entire
            # transfer's progress history as one blob at process exit
            # instead of live updates. Split on \r too, and feed every
            # completed segment through as soon as it arrives.
            buf = b""
            while True:
                chunk = await proc.stdout.read(65536)
                if not chunk:
                    if buf:
                        self._apply_progress(job, buf.decode("utf-8", "replace"))
                    break
                buf += chunk
                parts = re.split(rb"[\r\n]", buf)
                buf = parts.pop()
                for part in parts:
                    if part:
                        self._apply_progress(job, part.decode("utf-8", "replace"))

        async def read_stderr():
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", "replace").strip()
                if text:
                    stderr_tail.append(text)
                    del stderr_tail[:-20]

        await asyncio.gather(read_stdout(), read_stderr())
        returncode = await proc.wait()
        return returncode, stderr_tail

    async def _run_archive(self, job_id, job):
        """Run a compress or extract job, updating job.bytes_done as it goes."""
        loop = asyncio.get_running_loop()
        stderr_tail = []
        job.bytes_done = 0
        try:
            if job.mode == "compress":
                parent = os.path.dirname(job.sources[0].rstrip("/"))
                names = [os.path.basename(s.rstrip("/")) for s in job.sources]
                argv, cwd = archive.build_compress(job.dest, parent, names)
                kind = archive.archive_kind(job.dest)[0]
                tracker = archive.Tracker("compress", kind, parent)
                job.bytes_total = await loop.run_in_executor(
                    None, archive.tree_size, [os.path.join(parent, n) for n in names])
                stdin_file = None
            else:
                src = job.sources[0]
                os.makedirs(job.dest, exist_ok=True)
                argv, feed_stdin = archive.build_extract(src, job.dest)
                kind = archive.archive_kind(src)[0]
                cwd = None
                sizes = {}
                if kind == "zip":
                    sizes = await loop.run_in_executor(None, archive.zip_member_sizes, src)
                    job.bytes_total = sum(sizes.values())
                else:
                    job.bytes_total = os.path.getsize(src)
                tracker = archive.Tracker("extract", kind, None, sizes)
                stdin_file = src if feed_stdin else None
            proc = await asyncio.create_subprocess_exec(
                *argv, cwd=cwd,
                stdin=asyncio.subprocess.PIPE if stdin_file else asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        except (OSError, zipfile.BadZipFile) as exc:
            raise JobStartError(job.mode, exc)

        self.procs[job_id] = proc
        self.pgids[job_id] = os.getpgid(proc.pid)
        started = time.monotonic()

        async def read_stdout():
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                tracker.feed_line(line.decode("utf-8", "replace"))
                job.current_file = tracker.current_name[:400]

        async def read_stderr():
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", "replace").strip()
                if text:
                    stderr_tail.append(text)
                    del stderr_tail[:-20]

        async def feed():
            # tar reads the archive from stdin so the bytes handed over are
            # an exact progress counter, whatever the compression.
            try:
                with open(stdin_file, "rb") as fh:
                    while True:
                        chunk = await loop.run_in_executor(None, fh.read, 262144)
                        if not chunk:
                            break
                        proc.stdin.write(chunk)
                        await proc.stdin.drain()
                        job.bytes_done += len(chunk)
            except (OSError, ConnectionError):
                pass
            finally:
                with contextlib.suppress(OSError, ConnectionError):
                    proc.stdin.close()

        async def tick():
            while True:
                await asyncio.sleep(0.5)
                if stdin_file is None:
                    job.bytes_done = max(job.bytes_done, tracker.bytes_done(proc.pid))
                if job.state == "running":
                    elapsed = max(time.monotonic() - started, 0.001)
                    job.speed_bps = job.bytes_done / elapsed
                    left = job.bytes_total - job.bytes_done
                    job.eta_sec = int(left / job.speed_bps) if job.speed_bps > 0 and left >= 0 else -1

        ticker = asyncio.ensure_future(tick())
        tasks = [read_stdout(), read_stderr()]
        if stdin_file:
            tasks.append(feed())
        try:
            await asyncio.gather(*tasks)
            returncode = await proc.wait()
        finally:
            ticker.cancel()
        if returncode == 0 and job.state != "cancelled":
            job.bytes_done = max(job.bytes_done, job.bytes_total)
        return returncode, stderr_tail

    async def _finish_job(self, job_id, job, returncode, stderr_tail):
        tool = {"compress": "archive tool", "extract": "extract tool"}.get(job.mode, "rsync")
        async with self.lock:
            self.procs.pop(job_id, None)
            self.pgids.pop(job_id, None)
            self.running_ids.discard(job_id)
            if job.state != "cancelled":
                if returncode == 0:
                    job.state = "done"
                    if job.mode == "move":
                        self._cleanup_empty_dirs(job.sources)
                else:
                    job.state = "error"
                    job.error = ("\n".join(stderr_tail[-3:]) or ("%s exited with code %d" % (tool, returncode)))[:2000]
                job.finished_at = time.time()
            if job.mode == "compress" and job.state != "done":
                # Never leave a half-written archive behind.
                with contextlib.suppress(OSError):
                    os.unlink(job.dest)
            self._trim_history()
            self._save_nowait()
        await self._schedule()

    def _cleanup_empty_dirs(self, sources):
        for src in sources:
            if not os.path.isdir(src):
                continue
            for root, _dirs, _files in os.walk(src, topdown=False):
                with contextlib.suppress(OSError):
                    os.rmdir(root)

    def _apply_progress(self, job, line):
        line = line.strip("\r\n")
        if not line:
            return
        match = PROGRESS_RE.match(line)
        if match:
            bytes_str, percent_str, rate_val, rate_unit, eta_str = match.groups()
            job.bytes_done = int(bytes_str.replace(",", ""))
            percent = int(percent_str)
            job.bytes_total = int(job.bytes_done / (percent / 100.0)) if percent > 0 else -1
            job.speed_bps = _parse_size_rate(rate_val, rate_unit)
            job.eta_sec = _parse_eta(eta_str)
            return
        stripped = line.strip()
        if not stripped or stripped.startswith(SUMMARY_PREFIXES) or "%" in stripped:
            return
        job.current_file = stripped[:400]

    async def shutdown(self):
        async with self.lock:
            for pgid in list(self.pgids.values()):
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(pgid, signal.SIGCONT)
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(pgid, signal.SIGTERM)
            await self._save()


async def handle_client(daemon, reader, writer):
    sock = writer.get_extra_info("socket")
    uid = common.peer_uid(sock) if sock is not None else None
    # Fail closed: if the credential can't be established at all (missing
    # socket, SO_PEERCRED unsupported or denied), that's not "no evidence of
    # a mismatch", it's "no evidence of a match" -- reject rather than accept.
    if uid is None or uid != os.getuid():
        writer.close()
        return
    try:
        line = await reader.readuntil(b"\n")
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
        writer.close()
        return
    try:
        req = json.loads(line.decode("utf-8"))
        response = await daemon.handle(str(req.get("cmd", "")), req.get("args") or {})
    except (ValueError, TypeError) as exc:
        response = {"ok": False, "error": "bad request: " + str(exc)}
    with contextlib.suppress(ConnectionError, OSError):
        writer.write((json.dumps(response, separators=(",", ":")) + "\n").encode("utf-8"))
        await writer.drain()
    writer.close()


async def amain():
    logging.basicConfig(
        filename=common.log_file(), level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    with contextlib.suppress(OSError):
        os.chmod(common.log_file(), 0o600)

    daemon = TransferDaemon()
    daemon._load()
    await daemon._schedule()

    sock_path = common.socket_path()
    with contextlib.suppress(FileNotFoundError):
        os.unlink(sock_path)
    server = await asyncio.start_unix_server(
        lambda r, w: handle_client(daemon, r, w), path=sock_path, limit=common.MAX_LINE_BYTES,
    )
    os.chmod(sock_path, 0o600)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    async with server:
        await stop_event.wait()
        await daemon.shutdown()
        server.close()
        await server.wait_closed()
    with contextlib.suppress(FileNotFoundError):
        os.unlink(sock_path)


def main():
    asyncio.run(amain())


if __name__ == "__main__":
    main()
