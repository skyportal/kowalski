import fire
from typing import Mapping
import yaml


def patch_yaml(input_yaml: str, **kwargs):
    """Patch a yaml file using keyword arguments as in
    ./patch_yaml.py config.yaml --api-key=kjasdkjasd --password=blah --section.api=blink

    Nested fields must be separated with a dot (section.api -> config["section"]["api"])

    :param input_yaml:
    :param kwargs:
    :return:
    """
    with open(input_yaml) as yaml_file:
        content = yaml.load(yaml_file, Loader=yaml.FullLoader)

    if len(kwargs) == 0:
        return

    def patch(mapping: dict, update: Mapping):
        """Recursively update mapping with stuff from update

        :param mapping:
        :param update:
        :return:
        """
        for field, value in update.items():
            if "." not in field:
                mapping[field] = value
            else:
                fields = field.split(".")
                patch(mapping[fields[0]], {".".join(fields[1:]): value})

    patch(content, kwargs)

    # dump the updated file:
    with open(input_yaml, "w") as yaml_file:
        yaml.dump(content, yaml_file)


if __name__ == "__main__":
    fire.Fire(patch_yaml)
