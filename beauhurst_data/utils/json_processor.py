import json
from pathlib import Path
from typing import Callable

from utils.api_access import DATA


def load_json(input_path: str, lines_key: str = None, folder: str = "raw"):
    """
    open a json file and return contents as dict
    :param input_path: path to json file to open from data directory root or str which will be converted to path
    :param lines_key: if not none will load as json lines returning using this key in a dict
    :param folder: folder inside data folder to load from
    """
    if type(input_path) is str:
        input_path = Path(input_path)

    suffix = ".jsonl" if lines_key else ".json"
    input_path = input_path.with_suffix(suffix)

    if folder:
        input_path = folder / input_path

    with open(DATA / input_path) as f:
        if not lines_key:
            input_json = json.load(f)
        else:
            json_lines, input_lines = [], f.readlines()
            for line in input_lines:
                json_line = json.loads(line)
                json_lines.append(json_line)
            input_json = {lines_key: json_lines}
    return input_json


def save_json(
    output_json: dict, output_path: str, lines: bool = False, folder: str = None
):
    """
    Save a dict as json file
    :param output_json: a dict to be saved as json
    :param output_path: path to save json file to from data directory root or str which will be converted to path
    :param lines_key: if not none will save as json lines accessing this key in dict
    """

    output_path = Path(output_path)
    suffix = ".jsonl" if lines else ".json"
    output_path = output_path.with_suffix(suffix)

    if folder:
        output_path = folder / output_path

    data_path = DATA / output_path
    Path(data_path.parent).mkdir(parents=True, exist_ok=True)

    if not lines:
        with open(data_path, "w") as f:
            json.dump(output_json, f, indent=2)
    else:
        with open(data_path, "a") as f:
            json.dump(output_json, f)

