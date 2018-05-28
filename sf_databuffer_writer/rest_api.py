import json

import bottle
import logging

import os

_logger = logging.getLogger(__name__)


def register_rest_interface(app, manager):
    @app.get("/status")
    def get_status():
        return {"state": "ok",
                "status": manager.get_status()}

    @app.post("/parameters")
    def set_parameters():
        manager.set_parameters(bottle.request.json)

        return {"state": "ok",
                "status": manager.get_status(),
                "parameters": manager.get_parameters()}

    @app.get("/stop")
    def stop():
        manager.stop()

        return {"state": "ok",
                "status": manager.get_status()}

    @app.get("/kill")
    def kill():
        os._exit(0)

    @app.get("/statistics")
    def get_statistics():
        return {"state": "ok",
                "status": manager.get_status(),
                "statistics": manager.get_statistics()}

    @app.put("/start_pulse_id/<pulse_id>")
    def start_pulse_id(pulse_id):
        _logger.info("Received start_pulse_id %s.", pulse_id)

        manager.start_writer(int(pulse_id))

    @app.put("/stop_pulse_id/<pulse_id>")
    def stop_pulse_id(pulse_id):
        _logger.info("Received stop_pulse_id %s.", pulse_id)

        manager.stop_writer(int(pulse_id))

    @app.error(500)
    def error_handler_500(error):
        bottle.response.content_type = 'application/json'
        bottle.response.status = 200

        error_text = str(error.exception)

        _logger.error(error_text)

        return json.dumps({"state": "error",
                           "status": error_text})
