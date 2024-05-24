import datetime
import os

import uvloop
from aiohttp import web
from aiohttp_swagger3 import (
    ReDocUiSettings,
    SwaggerDocs,
    SwaggerInfo,
    SwaggerContact,
    SwaggerLicense,
)
from motor.motor_asyncio import AsyncIOMotorClient
from odmantic import AIOEngine

from kowalski.api.middlewares import (
    auth_middleware,
    error_middleware,
)
from kowalski.config import load_config
from kowalski.utils import (
    add_admin,
    init_db,
)
from kowalski.api.handlers import (
    PingHandler,
    UserHandler,
    UserTokenHandler,
    QueryHandler,
    FilterHandler,
    ZTFTriggerHandler,
    ZTFMMATriggerHandler,
    SkymapHandler,
    AlertCutoutHandler,
    AlertClassificationHandler,
)

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


async def app_factory():
    """
        App Factory
    :return:
    """
    # init db if necessary
    await init_db(config=config)

    # Database connection
    if config["database"]["srv"] is True:
        conn_string = "mongodb+srv://"
    else:
        conn_string = "mongodb://"

    if (
        config["database"]["admin_username"] is not None
        and config["database"]["admin_password"] is not None
    ):
        conn_string += f"{config['database']['admin_username']}:{config['database']['admin_password']}@"

    conn_string += f"{config['database']['host']}"
    if config["database"]["srv"] is not True:
        conn_string += f":{config['database']['port']}"

    if config["database"]["replica_set"] is not None:
        conn_string += f"/?replicaSet={config['database']['replica_set']}"

    client = AsyncIOMotorClient(
        conn_string,
        maxPoolSize=config["database"]["max_pool_size"],
    )
    mongo = client[config["database"]["db"]]

    # admin to connect to this instance from outside using API
    await add_admin(mongo, config=config)

    # app settings
    settings = {
        "client_max_size": config["server"].get("client_max_size", 1024**2),
    }

    # init app with auth and error handling middlewares
    app = web.Application(middlewares=[auth_middleware, error_middleware], **settings)

    # store mongo connection
    app["mongo"] = mongo

    # mark all enqueued tasks failed on startup
    await app["mongo"].queries.update_many(
        {"status": "enqueued"},
        {"$set": {"status": "error", "last_modified": datetime.datetime.utcnow()}},
    )

    # graciously close mongo client on shutdown
    async def close_mongo(_app):
        _app["mongo"].client.close()

    app.on_cleanup.append(close_mongo)

    # use ODMantic to work with structured data such as Filters
    engine = AIOEngine(client=client, database=config["database"]["db"])
    # ODM = Object Document Mapper
    app["mongo_odm"] = engine

    # set up JWT for user authentication/authorization
    app["JWT"] = {
        "JWT_SECRET": config["server"]["JWT_SECRET_KEY"],
        "JWT_ALGORITHM": config["server"]["JWT_ALGORITHM"],
        "JWT_EXP_DELTA_SECONDS": config["server"]["JWT_EXP_DELTA_SECONDS"],
    }

    # OpenAPI docs:
    s = SwaggerDocs(
        app,
        redoc_ui_settings=ReDocUiSettings(path="/docs/api/"),
        # swagger_ui_settings=SwaggerUiSettings(path="/docs/api/"),
        # rapidoc_ui_settings=RapiDocUiSettings(path="/docs/api/"),
        validate=config["misc"]["openapi_validate"],
        info=SwaggerInfo(
            title=config["server"]["name"],
            version=config["server"]["version"],
            description=config["server"]["description"],
            contact=SwaggerContact(
                name=config["server"]["contact"]["name"],
                email=config["server"]["contact"]["email"],
            ),
            license=SwaggerLicense(
                name="MIT",
                url="https://raw.githubusercontent.com/skyportal/kowalski/main/LICENSE",
            ),
        ),
        components=os.path.join("kowalski/api/components_api.yaml"),
    )

    # instantiate handler classes:
    ping_handler = PingHandler()
    user_handler = UserHandler()
    user_token_handler = UserTokenHandler()
    query_handler = QueryHandler()
    filter_handler = FilterHandler()
    ztf_trigger_handler = ZTFTriggerHandler()
    ztf_trigger_handler_test = ZTFTriggerHandler(test=True)
    ztf_mma_trigger_handler = ZTFMMATriggerHandler()
    ztf_mma_trigger_handler_test = ZTFMMATriggerHandler(test=True)
    skymap_handler = SkymapHandler()
    alert_cutout_handler = AlertCutoutHandler()
    alert_classification_handler = AlertClassificationHandler()

    routes = [
        web.get("/", ping_handler.get, name="root", allow_head=False),
        # auth:
        web.post("/api/auth", user_token_handler.post, name="auth"),
        # users:
        web.post("/api/users", user_handler.post),
        web.delete("/api/users/{username}", user_handler.delete),
        web.put("/api/users/{username}", user_handler.put),
        # queries:
        web.post("/api/queries", query_handler.post),
        # filters:
        web.get(
            r"/api/filters/{filter_id:[0-9]+}", filter_handler.get, allow_head=False
        ),
        web.post("/api/filters", filter_handler.post),
        web.patch("/api/filters", filter_handler.patch),
        web.delete("/api/filters/{filter_id:[0-9]+}", filter_handler.delete),
        # triggers
        web.get(
            "/api/triggers/ztf",
            ztf_trigger_handler.get,
            allow_head=False,
            name="ztf_triggers",
        ),
        web.put("/api/triggers/ztf", ztf_trigger_handler.put, name="ztf_triggers"),
        web.delete(
            "/api/triggers/ztf", ztf_trigger_handler.delete, name="ztf_triggers"
        ),
        web.get(
            "/api/triggers/ztf.test", ztf_trigger_handler_test.get, allow_head=False
        ),
        web.put("/api/triggers/ztf.test", ztf_trigger_handler_test.put),
        web.delete("/api/triggers/ztf.test", ztf_trigger_handler_test.delete),
        # mma triggers
        web.get("/api/triggers/ztfmma", ztf_mma_trigger_handler.get, allow_head=False),
        web.put("/api/triggers/ztfmma", ztf_mma_trigger_handler.put),
        web.delete("/api/triggers/ztfmma", ztf_mma_trigger_handler.delete),
        web.get(
            "/api/triggers/ztfmma.test",
            ztf_mma_trigger_handler_test.get,
            allow_head=False,
        ),
        web.put("/api/triggers/ztfmma.test", ztf_mma_trigger_handler_test.put),
        web.delete("/api/triggers/ztfmma.test", ztf_mma_trigger_handler_test.delete),
        # skymaps:
        web.put("/api/skymap", skymap_handler.put),
        web.get("/api/skymap", skymap_handler.get),
        web.delete("/api/skymap", skymap_handler.delete),
        # cutouts:
        web.get(
            "/api/cutouts/{survey}/{candid}/{cutout}/{file_format}",
            alert_cutout_handler.get,
            allow_head=False,
        ),
        # classifications:
        web.put(
            "/api/classifications/{survey}",
            alert_classification_handler.put,
        ),
        web.delete(
            "/api/classifications/{survey}",
            alert_classification_handler.delete,
        ),
        web.get(
            "/api/classifications/{survey}/{candid}",
            alert_classification_handler.get,
            allow_head=False,
        ),
    ]

    s.add_routes(routes)
    return app


uvloop.install()

if __name__ == "__main__":
    web.run_app(app_factory(), port=config["server"]["port"])
