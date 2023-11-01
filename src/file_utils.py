from pathlib import Path
from shutil import copy2
from urllib.request import urlretrieve

from pandas import Timestamp


def archive_data_file(file_path: Path, data_update_timestamp: Timestamp) -> None:
    data_dir = file_path.parent
    if data_dir.name != "data":
        raise Exception("Expected the file to be in the data dir")
    fn_parts = file_path.name.split(".")
    new_file_name = (
        f"{fn_parts[0]}_{data_update_timestamp.strftime('%Y_%m_%d__%H_%M_%S')}.{fn_parts[1]}"
    )
    copy2(src=file_path, dst=data_dir.joinpath("archive", new_file_name))
