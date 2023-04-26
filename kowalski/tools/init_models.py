import os

from kowalski.config import load_config
from kowalski.log import log

config = load_config(config_files=["config.yaml"])

if not os.path.exists("models"):
    os.mkdir("models")

ml = config["kowalski"]["ml"]


def init_models():
    for instrument in ml.keys():
        if not os.path.exists(os.path.join("models", instrument.lower())):
            os.mkdir(os.path.join("models", instrument.lower()))

        if "models" not in ml[instrument].keys():
            raise KeyError(
                f"Instrument {instrument} has been added to the ML config, but no models have been specified."
            )

        existing_models = []
        downloaded_models = []
        for model_name in ml[instrument]["models"]:
            model = ml[instrument]["models"][model_name]
            if "url" not in model.keys():
                print(
                    f"Model {model_name} for instrument {instrument} has no URL specified. Skipping."
                )
                continue

            if "version" not in model.keys():
                raise KeyError(
                    f"Model {model_name} for instrument {instrument} has no version specified."
                )

            format = "h5"
            if "format" in model.keys():
                if model["format"] not in ["h5", "pb"]:
                    raise ValueError(
                        f"Model {model_name} for instrument {instrument} has an invalid format specified. Must be either 'fits' or 'h5'."
                    )
                format = model["format"]

            if format == "h5":
                name = f"{model_name}.{model['version']}.h5"
                # first check if the model already exists
                if os.path.exists(os.path.join("models", instrument.lower(), name)):
                    print(
                        f"Model {model_name} for instrument {instrument} already exists. Skipping."
                    )
                    existing_models.append(name)
                    continue
                if "://" not in model["url"]:
                    # its a path on disk, so copy the file over
                    os.system(
                        f"cp {model['url']} {os.path.join('models', instrument.lower(), name)}"
                    )
                else:
                    os.system(
                        f"wget -O {os.path.join('models', instrument.lower(), name)} {model['url']}"
                    )
                downloaded_models.append(name)

            if format == "pb":
                # here instead, we want to download the model (which will be a compressed tarball) and extract it in a directory with the name of the model
                name = f"{model_name}.{model['version']}"
                # first check if the model already exists
                if os.path.exists(os.path.join("models", instrument.lower(), name)):
                    print(
                        f"Model {model_name} for instrument {instrument} already exists. Skipping."
                    )
                    existing_models.append(name)
                    continue
                if "://" not in model["url"]:
                    # its a path on disk, so copy the file over
                    os.system(
                        f"cp {model['url']} {os.path.join('models', instrument.lower(), name + '.tar.gz')}"
                    )
                else:
                    os.system(
                        f"wget -O {os.path.join('models', instrument.lower(), name + '.tar.gz')} {model['url']}"
                    )
                os.mkdir(os.path.join("models", instrument.lower(), name))
                os.system(
                    f"tar -xvf {os.path.join('models', instrument.lower(), name + '.tar.gz')} -C {os.path.join('models', instrument.lower(), name)} --strip-components 1"
                )
                downloaded_models.append(name)

        print(
            f"{instrument} models: {len(downloaded_models)} downloaded, {len(existing_models)} already existed."
        )


if __name__ == "__main__":
    log("Initializing models...")
    init_models()
    print()
    print("-" * 20)
    print()
