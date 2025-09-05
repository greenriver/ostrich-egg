import os
from conftest import ROOT_DIR


def test_that_schema_is_updated():
    schema_file = os.path.join(ROOT_DIR, "schemas", "config_schema.json")
    with open(schema_file, "r") as f:
        data = f.read()
    from schemas.generate import write_config_to_file

    write_config_to_file()
    with open(schema_file, "r") as f:
        new_data = f.read()
    assert (
        data == new_data
    ), "You changed the json schema for configs, please run `python schemas/generate.py` to update the schema."
