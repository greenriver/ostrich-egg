import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from ostrich_egg.config import Config
from pydantic import BaseModel

DIR = os.path.dirname(__file__)


def write_model_to_file(model: BaseModel, file_path: str):
    with open(file_path, "w") as f:
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
        } | model.model_json_schema()
        json.dump(obj=schema, fp=f, indent=2)
        print(os.path.abspath(f.name))


def write_config_to_file(directory=DIR):

    for model, file_path in [
        (
            Config,
            os.path.join(directory, "config_schema.json"),
        ),
    ]:
        write_model_to_file(model=model, file_path=file_path)


if __name__ == "__main__":
    write_config_to_file()
