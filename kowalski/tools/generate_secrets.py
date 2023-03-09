from cryptography import fernet
from kowalski.utils import uid


if __name__ == "__main__":
    secrets = dict()

    fernet_key = fernet.Fernet.generate_key().decode()
    aiohttp_secret_key = uid(32)
    jwt_secret_key = uid(32)

    for key in ("server", "misc"):
        if key not in secrets:
            secrets[key] = dict()

    secrets["server"]["SECRET_KEY"] = aiohttp_secret_key
    secrets["server"]["JWT_SECRET_KEY"] = jwt_secret_key
    secrets["misc"]["fernet_key"] = fernet_key
