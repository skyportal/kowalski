import questionary
import yaml

from kowalski.config import load_config

default_config = load_config(["config.default.yaml"])

def validate(key, value):
    if key in ['host', 'admin_username', 'admin_password', 'db']:
        if not value or not isinstance(value, str) or len(value.strip()) == 0:
            raise ValueError("Database must be a non-empty string")
    elif key == 'build_indexes':
        try:
            value = bool(value)
        except:
            raise ValueError("build_indexes must be a boolean")
    return value

def prompt(key):
    match = {
        'host': questionary.text("MongoDB Host (URI)"),
        'admin_username': questionary.text("Username", default=default_config['kowalski']['database']['admin_username']),
        'admin_password': questionary.password("Password", default=default_config['kowalski']['database']['admin_password']),
        'db': questionary.text("Database", default=default_config['kowalski']['database']['db']),
        'build_indexes': questionary.confirm("Build indexes", default=default_config['kowalski']['database']['build_indexes'])
    }
    return match[key].ask()

def prompt_db():
    db = {}
    for key in ['host', 'admin_username', 'admin_password', 'db', 'build_indexes']:
        while True:
            try:
                db[key] = prompt(key)
                db[key] = validate(key, db[key])
                break
            except ValueError as e:
                print(e)
                continue
    return db

db = prompt_db()


db['srv'] = True

with open("config.yaml", "w") as f:
    yaml.dump({'kowalski': {'database': db}}, f)

print("config.yaml written successfully! You can now run 'make run' to start Kowalski, or `make run_api` to only start the API server.")

