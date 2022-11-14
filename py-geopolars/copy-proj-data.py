import os
import shutil
import sys
from pathlib import Path


def get_data_dir() -> str:
    """
    Taken and adapted from pyproj
    """

    def valid_data_dir(potential_data_dir):
        if (
            potential_data_dir is not None
            and Path(potential_data_dir, "proj.db").exists()
        ):
            return True
        return False

    proj_exe = shutil.which("proj", path=sys.prefix)
    if proj_exe is None:
        proj_exe = shutil.which("proj")
    if proj_exe is not None:
        system_proj_dir = Path(proj_exe).parent.parent / "share" / "proj"
        if valid_data_dir(system_proj_dir):
            _VALIDATED_PROJ_DATA = str(system_proj_dir)

    if _VALIDATED_PROJ_DATA is None:
        raise Exception(
            "Valid PROJ data directory not found. "
            "Either set the path using the environmental variable "
            "PROJ_DATA (PROJ 9.1+) | PROJ_LIB (PROJ<9.1) or "
            "with `pyproj.datadir.set_data_dir`."
        )
    return _VALIDATED_PROJ_DATA


def copy_proj_data():
    data_dir = "python/geopolars"
    os.makedirs(data_dir, exist_ok=True)
    proj_data_dir = Path(data_dir) / "proj_data"
    if proj_data_dir.exists():
        shutil.rmtree(proj_data_dir)
    shutil.copytree(get_data_dir(), proj_data_dir)


if __name__ == "__main__":
    copy_proj_data()
