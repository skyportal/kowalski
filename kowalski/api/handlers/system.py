from aiohttp import web

from kowalski.api.middlewares import (
    auth_required,
)
from .base import BaseHandler


class PingHandler(BaseHandler):
    """Handler for pinging the server"""

    # @routes.get('/', name='ping', allow_head=False)
    @auth_required
    async def get(self, request: web.Request) -> web.Response:
        """Ping the server

        :param request:
        :return:
        ---
        summary: Ping the server
        tags:
          - system

        responses:
          '200':
            description: greetings to an authorized user
            content:
              application/json:
                schema:
                  type: object
                  required:
                    - status
                    - message
                  properties:
                    status:
                      type: string
                    message:
                      type: string
                  example:
                    status: success
                    message: greetings from Kowalski!
          '401':
            description: unauthorized
            content:
              application/json:
                schema:
                  type: object
                  required:
                    - status
                    - message
                  properties:
                    status:
                      type: string
                    message:
                      type: string
                  example:
                    status: error
                    message: unauthorized
        """
        return web.json_response(
            {"status": "success", "message": "greetings from Kowalski!"}, status=200
        )
