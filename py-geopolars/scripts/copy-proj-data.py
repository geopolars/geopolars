"""
Copy proj data into local directory to be included in wheels
"""
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional, Set

from pkg_resources import parse_version

PROJ_MIN_VERSION = parse_version("9.1.0")
CURRENT_FILE_PATH = Path(__file__).absolute().parent
INTERNAL_PROJ_DATA_DIR = CURRENT_FILE_PATH / ".." / "python" / "geopolars" / "proj_data"
PROJ_VERSION_SEARCH = re.compile(r".*Rel\.\s+(?P<version>\d+\.\d+\.\d+).*")


# In some proj installations (e.g. at least homebrew), the full proj-data files are
# included in the installation dir. This is 600MB, so instead we only copy to the output
# directory a whitelist from what pyproj appears to include in its wheels.
FILES_TO_COPY: Set[str] = {
    "CH",
    "GL27",
    "ITRF2000",
    "ITRF2008",
    "ITRF2014",
    "deformation_model.schema.json",
    "nad.lst",
    "nad27",
    "nad83",
    "other.extra",
    "proj.db",
    "proj.ini",
    "projjson.schema.json",
    "triangulation.schema.json",
    "world",
}


def get_proj_version(proj_dir: Path) -> str:
    proj_version = os.environ.get("PROJ_VERSION")
    if proj_version:
        return proj_version
    proj = proj_dir / "bin" / "proj"
    proj_ver = subprocess.check_output(str(proj), stderr=subprocess.STDOUT).decode(
        "ascii"
    )
    match = PROJ_VERSION_SEARCH.search(proj_ver)
    if not match:
        raise SystemExit(
            "PROJ version unable to be determined. "
            "Please set the PROJ_VERSION environment variable."
        )
    return match.groupdict()["version"]


def check_proj_version(proj_version: str) -> None:
    """checks that the PROJ library meets the minimum version"""
    if parse_version(proj_version) < PROJ_MIN_VERSION:
        raise SystemExit(
            f"ERROR: Minimum supported PROJ version is {PROJ_MIN_VERSION}, installed "
            f"version is {proj_version}."
        )


def get_proj_dir() -> Path:
    """
    This function finds the base PROJ directory.
    """
    proj_dir_environ = os.environ.get("PROJ_DIR")
    proj_dir: Optional[Path] = None

    if proj_dir_environ is not None:
        proj_dir = Path(proj_dir_environ)

    elif proj_dir is None:
        proj = shutil.which("proj", path=sys.prefix)
        if proj is None:
            proj = shutil.which("proj")
        if proj is None:
            raise SystemExit(
                "proj executable not found. Please set the PROJ_DIR variable. "
                "For more information see: "
                "https://pyproj4.github.io/pyproj/stable/installation.html"
            )
        proj_dir = Path(proj).parent.parent

    elif proj_dir is not None and proj_dir.exists():
        print("PROJ_DIR is set, using existing PROJ installation..\n")

    else:
        raise SystemExit(f"ERROR: Invalid path for PROJ_DIR {proj_dir}")

    return proj_dir


def copy_proj_data(proj_dir: Path):
    source_data_dir = proj_dir / "share" / "proj"
    if not (source_data_dir / "proj.db").exists():
        msg = (
            f"Expected to find proj.db at {source_data_dir}."
            f"Found: {list(source_data_dir.iterdir())}"
        )
        raise ValueError(msg)

    if INTERNAL_PROJ_DATA_DIR.exists():
        shutil.rmtree(INTERNAL_PROJ_DATA_DIR)

    INTERNAL_PROJ_DATA_DIR.mkdir(parents=True)

    for fname in FILES_TO_COPY:
        source_path = source_data_dir / fname
        dest_path = INTERNAL_PROJ_DATA_DIR / fname
        if source_path.exists:
            shutil.copy(str(source_path), str(dest_path))


def main():
    # By default we'll try to get options PROJ_DIR or the local version of proj
    proj_dir = get_proj_dir()
    proj_version = get_proj_version(proj_dir)
    check_proj_version(proj_version)

    copy_proj_data(proj_dir)


if __name__ == "__main__":
    main()
