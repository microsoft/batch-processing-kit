# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import multiprocessing
from http import HTTPStatus
from threading import Thread
from typing import Tuple, Optional
import logging

from flask import Flask, Response, Blueprint, request

from .batch_request import BatchRequest
from .batch_status import BatchStatusProvider, BatchStatus, BatchStatusEnum
from .utils import BadRequestError, BatchNotFoundException


logger = logging.getLogger("batch")

flask_app_name = 'batch_apiserver'
flask_module_name = '__batch_apiserver__'
bp = Blueprint(flask_app_name, flask_module_name)


class ApiServer(object):
    """
    The primary controller by which BatchRequests are submitted and queried for status.
    This can be consumed directly as a client object in consumer python apps or through
    the HTTP endpoints.

    status(batch_id: int) -> BatchStatus
    GET /status?batch_id=<batch_id>
        (if add_flask_functional=True)
        Request arg is the id of the batch request for which to get status.
        Response body will contain a JSON dict with the following fields
            {'batch_id': <batch_id>,
             'status': '<waiting|running|done>',
             'run_summary': ...,
             'results_path': ... }


    submit(req: BatchRequest) -> BatchStatus
    POST /submit
        (if add_flask_functional=True)
        Request body should contain a JSON dict like follows:
        {'files': ['/path/to/A', '/path/to/B', ...],
         'type': '<SpeechSDKBatchRequest|GrpcBatchRequest>'
         ... Other fields particular to the 'type' ...
        }
        Response body identical to status(batch_id) with the batch's new id assignment.


    watch(batch_id: int, target_state: BatchStatusEnum) -> BatchStatus
        (Blocking)
    GET /watch?batch_id=<batch_id>&target_state=<waiting|running|done>
        (if add_flask_functional=True)
        (HTTP Long Poll)
        Identical to status() but will block until there is change in status
        to `target_state` or beyond.


    GET /health
        (if add_flask_probe=True)
        No params.
        Returns HTTP Status OK (200) if the process is fine. Useful as process readiness probe.
    """

    def __init__(
            self,
            submission_queue: multiprocessing.Queue,
            status_provider: BatchStatusProvider,
            add_flask_functional: bool = True,
            add_flask_probe: bool = True,
            port: int = 5000):
        """
        Construct apiserver via dependency injection.
        :param submission_queue: Drop-point for new BatchRequests to be executed by downstream component.
        :param status_provider: Getter and setter for creating BatchRequests and querying their progress.
        :param add_flask_functional: whether to also run the http server for
                                     batch submission, querying (see class doc).
        :param add_flask_probe: whether to add http server for health probe endpoint.
        :param port: port on which to listen for incoming http requests.
        """
        self.submission_queue = submission_queue
        self.status_provider = status_provider
        if add_flask_probe or add_flask_functional:
            self.flask_app = Flask(flask_app_name)
            if add_flask_functional:
                self._register_functional_http_endpoints()
            if add_flask_probe:
                self._register_probe_http_endpoints()
            # Running the Flask app will take place in a forked subprocess and
            # the thread invoking run() will not return until the process terminates
            # which is when we are done with the top-level process (calling this), so we
            # we do the subprocess wait in a daemon thread.
            Thread(
                target=self.flask_app.run,
                kwargs={
                    "host": "localhost", "port": port,
                    "debug": True, "use_reloader": False},
                name="MainProc_FlaskAppParentThread",
                daemon=True
            ).start()

    def submit(self, req: BatchRequest) -> BatchStatus:
        """
        Submit a new job.
        """
        logger.info(
            "ApiServer: submit() request:  {0}".format(req.serialize_json())
        )
        self.status_provider.new_batch(req)
        self.submission_queue.put(req)
        return self.status(req.batch_id)

    def status(self, batch_id: int) -> BatchStatus:
        return self.status_provider.status(batch_id)

    def watch(self, batch_id: int, target_state: BatchStatusEnum, timeout: Optional[float] = None) -> BatchStatus:
        """
        Synchronously blocks the request thread until we transition to target state or after, or
        returns immediately if we already meet that criteria. Only a `timeout` permits
        returning without reaching the target state, in which case the consumer must check
        the returned BatchStatus.
        """
        logger.info(
            "ApiServer: watch() request:  batch_id: {0}, target_state: {1}, timeout: {2}".format(
                batch_id, target_state, timeout
            )
        )

        event: multiprocessing.Event = self.status_provider.register_watch(batch_id, target_state)
        event.wait(timeout)
        return self.status(batch_id)

    def _submit_controller(self) -> Response:
        """
        HTTP wrapper around submit()
        :returns: json-serialized BatchStatus of the newly submitted batch
        """
        body = request.get_json(force=True, silent=False)
        logger.info("ApiServer: Received [POST] /submit : {0}".format(body))
        req = BatchRequest.from_json(body)
        logger.info("ApiServer: New Submission: {0}".format(req.serialize_json()))
        status = self.submit(req)
        response = Response(status=200)
        response.stream.write(status.serialize_json())
        return response

    def _status_controller(self):
        """
        HTTP wrapper around status()
        """
        batch_id = self._id_from_request()
        logger.info("[GET] /status : {0}".format(batch_id))
        response = Response(status=200)
        response.stream.write(self.status(batch_id))
        return response

    def _watch_controller(self):
        """
        HTTP wrapper around watch()
        """
        batch_id = self._id_from_request()
        logger.info("[GET] /watch : {0}".format(batch_id))
        try:
            target_state: BatchStatusEnum = self._target_state_from_request()
            timeout: Optional[float] = self._timeout_from_request()
            status: BatchStatus = self.watch(batch_id, target_state, timeout)
            response = Response(status=200)
            response.stream.write(status.serialize_json())
            return response
        except BadRequestError as err:
            logger.warning("Bad request to /watch : {0}".format(str(err)))

    def _ready_controller(self):
        """
        Just confirms this process is running and built-in server + web app is taking requests.
        """
        response = Response(status=200)
        response.stream.write("true")
        return response

    def _id_from_request(self) -> int:
        """
        Extract id from the active GET request and verifies existence.
        """
        batch_id = request.args.get('batch_id')  # 'request' module reflects on this request thread.
        if batch_id is None:
            raise BadRequestError("Arg 'batch_id' was not supplied")
        return int(batch_id)

    def _target_state_from_request(self) -> BatchStatusEnum:
        """
        Extract target_state from the active GET request and verifies correctness.
        """
        target_state = request.args.get('target_state')
        if target_state is None:
            raise BadRequestError("Arg 'target_state' was not supplied")
        possible_states = [e.name for e in BatchStatusEnum]
        if target_state not in possible_states:
            raise BadRequestError(
                "'target_state': {target_state} is invalid. Options are: {options}".format(
                    target_state=target_state,
                    options=possible_states
                )
            )
        return BatchStatusEnum[target_state]

    def _timeout_from_request(self) -> Optional[float]:
        """
        Extract timeout from the active GET request and verifies correctness.
        """
        timeout = request.args.get('timeout')
        if timeout is None:
            return None
        try:
            timeout = float(timeout)
        except ValueError as err:
            raise BadRequestError("'timeout': {0} is invalid (must be numeric)".format(timeout))
        return timeout

    def _register_functional_http_endpoints(self):
        """
        Set up routes and verbs for controllers to do with submitting and querying batches.
        """
        self.flask_app.add_url_rule('/submit', 'submit', self._submit_controller, methods=["POST"])
        self.flask_app.add_url_rule('/status', 'status', self._status_controller, methods=["GET"])
        self.flask_app.add_url_rule('/watch', 'watch', self._watch_controller, methods=["GET"])
        self.flask_app.register_error_handler(Exception, self._code_exception)

    def _register_probe_http_endpoints(self):
        """
        Set up routes and verbs for healthcheck probes.
        """
        self.flask_app.add_url_rule('/ready', 'ready', self._ready_controller, methods=["GET"])
        self.flask_app.register_error_handler(Exception, self._code_exception)

    def _code_exception(self, e: Exception) -> Tuple[str, int]:
        """
        Provide proper HTTP status codes with exceptions where appropriate.
        """
        if isinstance(e, BadRequestError):
            return str(e), HTTPStatus.BAD_REQUEST
        if isinstance(e, BatchNotFoundException):
            return str(e), HTTPStatus.NOT_FOUND
        return str(e), HTTPStatus.INTERNAL_SERVER_ERROR
