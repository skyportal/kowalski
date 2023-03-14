import os

from kowalski.utils import load_config, log

config = load_config(config_file="config.yaml")

# here, we want to grab the instruments from the 'ml' key in the config, and for each instrument download the models if a link is provided

# for each instrument, we want to create a directory in the models directory with its name lowercased

# then, we want to download the models into that directory

# first, if the models directory doesn't exist, create it

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
                os.system(
                    f"wget -O {os.path.join('models', instrument.lower(), name)} {model['url']}"
                )
                downloaded_models.append(name)

            if format == "pb":
                # here instead, we want to download the model (which will be a compressed tarball) and extract it in a directory with the name of the model
                name = f"{model_name}.{model['version']}.pb"
                # first check if the model already exists
                if os.path.exists(os.path.join("models", instrument.lower(), name)):
                    print(
                        f"Model {model_name} for instrument {instrument} already exists. Skipping."
                    )
                    existing_models.append(name)
                    continue
                os.system(
                    f"wget -O {os.path.join('models', instrument.lower(), name)} {model['url']}"
                )
                os.system(
                    f"tar -xvf {os.path.join('models', instrument.lower(), name)} -C {os.path.join('models', instrument.lower())}"
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
