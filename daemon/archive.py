"""Archive (compress / extract) helpers for filetransferd.

Pure functions plus a small progress tracker, kept apart from the daemon's
event loop so they can be unit tested. The daemon runs the commands built
here with an explicit argv (never a shell) and a `--` separator before any
user-supplied name.

Progress model:
  - compress: total is the summed size of the source files. tar/zip print each
    name as they start it, so finished files add their size and the file being
    read adds its current read offset (from /proc/<pid>/fdinfo).
  - extract (tar): the archive is fed to tar over stdin by the daemon, so the
    bytes fed over the archive size is the progress.
  - extract (zip): unzip prints each member it writes; totals come from the
    zip's central directory.
"""
import os
import re
import shutil
import zipfile

MAX_WALK_ENTRIES = 5_000_000
MAX_ZIP_ENTRIES = 5_000_000

_TAR_FLAGS = {
    ".tar.gz": ["-z"],
    ".tgz": ["-z"],
    ".tar.xz": ["-J"],
    ".txz": ["-J"],
    ".tar.zst": ["--zstd"],
    ".tar.bz2": ["-j"],
    ".tbz2": ["-j"],
    ".tar": [],
}

_ZIP_ADD_RE = re.compile(r"^\s*adding:\s+(.*?)(?:\s+\((?:deflated|stored)[^)]*\))?\s*$")
_ZIP_EXTRACT_RE = re.compile(r"^\s*(?:inflating|extracting|linking):\s+(.*?)\s*$")


def archive_kind(path):
    """("zip", []) or ("tar", flags) for a supported archive name, else None."""
    lower = path.lower()
    if lower.endswith(".zip"):
        return ("zip", [])
    for suffix in sorted(_TAR_FLAGS, key=len, reverse=True):
        if lower.endswith(suffix):
            return ("tar", list(_TAR_FLAGS[suffix]))
    return None


def tool_path(name):
    """Absolute path of a known tool, or None. Never a bare $PATH name."""
    return shutil.which(name)


def _line_buffered(argv):
    stdbuf = tool_path("stdbuf")
    return [stdbuf, "-oL"] + argv if stdbuf else argv


def tree_size(paths):
    """Total bytes of regular files under `paths` (symlinks are not followed)."""
    total = 0
    seen = 0
    for top in paths:
        try:
            st = os.lstat(top)
        except OSError:
            continue
        if not os.path.isdir(top) or os.path.islink(top):
            total += st.st_size if os.path.isfile(top) or os.path.islink(top) else 0
            continue
        for root, dirs, files in os.walk(top, followlinks=False):
            for name in files:
                seen += 1
                if seen > MAX_WALK_ENTRIES:
                    return total
                try:
                    total += os.lstat(os.path.join(root, name)).st_size
                except OSError:
                    pass
    return total


def zip_member_sizes(archive):
    """{member name: uncompressed size} from a zip's central directory."""
    sizes = {}
    with zipfile.ZipFile(archive) as zf:
        for i, info in enumerate(zf.infolist()):
            if i >= MAX_ZIP_ENTRIES:
                break
            sizes[info.filename] = info.file_size
    return sizes


def build_compress(archive, parent, names):
    """(argv, cwd) that creates `archive` from `names` (relative to `parent`)."""
    kind, flags = archive_kind(archive)
    if kind == "tar":
        tar = tool_path("tar")
        if not tar:
            raise FileNotFoundError("tar not found")
        return [tar, "-c", *flags, "-v", "-f", archive, "-C", parent, "--", *names], parent
    zip_bin = tool_path("zip")
    if not zip_bin:
        raise FileNotFoundError("zip not found")
    return _line_buffered([zip_bin, "-r", archive, "--", *names]), parent


def build_extract(archive, dest):
    """(argv, uses_stdin_feed) that extracts `archive` into `dest`, never
    overwriting existing files."""
    kind, flags = archive_kind(archive)
    if kind == "tar":
        tar = tool_path("tar")
        if not tar:
            raise FileNotFoundError("tar not found")
        return [tar, "-x", *flags, "--skip-old-files", "--no-same-owner", "-C", dest, "-f", "-"], True
    unzip = tool_path("unzip")
    if not unzip:
        raise FileNotFoundError("unzip not found")
    return _line_buffered([unzip, "-n", "-d", dest, archive]), False


def read_fd_offset(pid, path):
    """Current read offset of `pid`'s open fd on `path`, or None."""
    fd_dir = "/proc/%d/fd" % pid
    try:
        fds = os.listdir(fd_dir)
    except OSError:
        return None
    for fd in fds:
        try:
            if os.readlink(os.path.join(fd_dir, fd)) != path:
                continue
            with open("/proc/%d/fdinfo/%s" % (pid, fd), "rb") as fh:
                for line in fh.read(512).decode("ascii", "replace").splitlines():
                    if line.startswith("pos:"):
                        return int(line.split()[1])
        except (OSError, ValueError, IndexError):
            continue
    return None


class Tracker:
    """Turns tar/zip/unzip's per-member output lines into a byte count."""

    def __init__(self, mode, kind, parent, sizes=None):
        self.mode = mode          # "compress" or "extract"
        self.kind = kind          # "tar" or "zip"
        self.parent = parent      # compress: directory the names are relative to
        self.sizes = sizes or {}  # extract/zip: member name -> size
        self.completed = 0
        self.current = None       # compress: absolute path being read
        self.current_name = ""
        self.current_size = 0

    def _size_of(self, name):
        if self.mode == "compress":
            try:
                st = os.lstat(os.path.join(self.parent, name))
            except OSError:
                return 0
            return st.st_size if not os.path.isdir(os.path.join(self.parent, name)) else 0
        return self.sizes.get(name, 0)

    def feed_line(self, line):
        """Account for one output line. Returns the member name or None."""
        line = line.rstrip("\r\n")
        if not line:
            return None
        if self.mode == "compress":
            if self.kind == "zip":
                match = _ZIP_ADD_RE.match(line)
                if not match:
                    return None
                name = match.group(1)
            else:
                name = line
        else:
            match = _ZIP_EXTRACT_RE.match(line)
            if not match:
                return None
            name = match.group(1)
        self.completed += self.current_size
        self.current_name = name
        self.current_size = self._size_of(name)
        self.current = os.path.join(self.parent, name) if self.mode == "compress" else None
        return name

    def bytes_done(self, pid=None):
        done = self.completed
        if self.mode == "compress" and self.current and pid:
            pos = read_fd_offset(pid, self.current)
            if pos is not None:
                done += min(pos, self.current_size)
        return done
