import sys
from pathlib import Path
from typing import Optional

if sys.version_info >= (3, 11):
    from importlib.resources import files
else:
    from importlib_resources import files


def get_proj_data_path() -> Optional[Path]:
    local_proj_data = Path(files("geopolars") / "proj_data")
    if local_proj_data.exists():
        return local_proj_data

    return None


PROJ_DATA_PATH: Optional[Path] = get_proj_data_path()
