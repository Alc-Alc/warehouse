# SPDX-License-Identifier: Apache-2.0

import contextlib
import gc
import logging
import socket
import threading
import types
import uuid

import celery._state
import celery.worker.state
import pytest
import redis

from tests.conftest import get_app_config

REDIS_HOST = "redis"
REDIS_PORT = 6379


class _OutageProxy:
    """A TCP proxy in front of Redis that can take the broker away."""

    def __init__(self):
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        self._lock = threading.Lock()
        self._sockets = []
        self._pubsub = []
        self._pumps = []
        self._replies_flow = threading.Event()
        self._replies_flow.set()
        self._connected = threading.Event()
        self._refuse_next = False
        self._stopping = False
        self.refused = threading.Event()
        self._acceptor = threading.Thread(target=self._accept, daemon=True)
        self._acceptor.start()

    def _accept(self):
        while True:
            client, _ = self._listener.accept()
            if self._stopping:
                client.close()
                break
            if self._refuse_next:
                self._refuse_next = False
                client.close()
                self.refused.set()
                continue
            upstream = socket.create_connection((REDIS_HOST, REDIS_PORT))
            pumps = [
                threading.Thread(
                    target=self._forward_requests, args=(client, upstream), daemon=True
                ),
                threading.Thread(
                    target=self._forward_replies, args=(upstream, client), daemon=True
                ),
            ]
            with self._lock:
                self._sockets += [client, upstream]
                self._pumps += pumps
            for pump in pumps:
                pump.start()
            self._connected.set()

    def _forward_requests(self, client, upstream):
        with contextlib.suppress(OSError):
            while data := client.recv(65536):
                if b"SUBSCRIBE" in data:
                    with self._lock:
                        self._pubsub += [client, upstream]
                upstream.sendall(data)
        self._hang_up([client, upstream])

    def _forward_replies(self, upstream, client):
        with contextlib.suppress(OSError):
            while data := upstream.recv(65536):
                self._replies_flow.wait()
                client.sendall(data)
        self._hang_up([client, upstream])

    @staticmethod
    def _hang_up(sockets):
        # shutdown() wakes a recv() blocked in another thread; on Linux a bare
        # close() does not.
        for sock in sockets:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)

    def outage(self):
        """
        Lose the broker the way that leaves the worker's task channel half-closed.

        The worker has to notice the dead pub/sub connection (remote control's,
        on its own channel) while a BRPOP is still pending on the task channel.
        Closing the task channel then fails in the BRPOP drain, and because the
        broker is still away when the remote control channel is closed next, the
        task channel is not closed again before the connection is given up.
        """
        self._replies_flow.clear()
        self._connected.clear()
        with self._lock:
            pubsub = list(self._pubsub)
        assert pubsub, "the worker has no pub/sub connection"
        self._hang_up(pubsub)
        # redis-py reconnects the pub/sub connection inside the failed read and
        # waits for the handshake reply, which is being held back. That parks
        # the worker in its error handling until everything else is gone.
        assert self._connected.wait(timeout=10), "pub/sub reconnect not seen"
        self._refuse_next = True
        with self._lock:
            sockets = list(self._sockets)
        self._hang_up(sockets)
        self._replies_flow.set()

    def stop(self):
        self._stopping = True
        socket.create_connection(("127.0.0.1", self.port)).close()
        self._acceptor.join(timeout=10)
        self._hang_up(self._sockets)
        self._replies_flow.set()
        for pump in self._pumps:
            pump.join(timeout=10)
        for sock in [*self._sockets, self._listener]:
            sock.close()


@pytest.fixture
def broker_proxy():
    proxy = _OutageProxy()
    yield proxy
    proxy.stop()


@pytest.fixture
def worker(database, broker_proxy):
    """A worker for warehouse's Celery app, reaching Redis through the proxy."""
    config = get_app_config(
        database,
        nondefaults={
            "celery.broker_redis_url": f"redis://127.0.0.1:{broker_proxy.port}/0"
        },
    )
    celery_app = config.registry["celery.app"]
    key_prefix = f"{uuid.uuid4()}:"
    celery_app.conf.broker_transport_options = {"global_keyprefix": key_prefix}

    started = threading.Event()
    release = threading.Event()
    ran = threading.Event()

    @celery_app.task(name="tests.block", acks_late=True)
    def block(request):
        started.set()
        release.wait(timeout=60)

    @celery_app.task(name="tests.marker", acks_late=True)
    def marker(request):
        ran.set()

    def send(name):
        # Straight to Redis, so the proxy only carries the worker's connections.
        with celery_app.connection_for_write(
            url=f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
        ) as connection:
            celery_app.send_task(name, connection=connection)

    # The pool's threads find the app through Celery's default app.
    previous_default_app = celery._state.default_app
    celery_app.set_default()
    ready = threading.Event()
    # Remote control stays enabled: `_OutageProxy.outage` depends on it.
    controller = celery_app.WorkController(
        pool="threads",
        concurrency=1,
        ready_callback=lambda consumer: ready.set(),
        without_gossip=True,
        without_mingle=True,
        without_heartbeat=True,
    )
    thread = threading.Thread(target=controller.start, daemon=True)
    thread.start()
    assert ready.wait(timeout=10)

    yield types.SimpleNamespace(send=send, started=started, release=release, ran=ran)

    release.set()
    celery.worker.state.should_stop = 0
    thread.join(timeout=15)
    celery.worker.state.should_stop = None
    celery._state.set_default_app(previous_default_app)
    celery_app.close()
    # A surviving worker leaves the pre-outage channel behind, and collecting it
    # restores its unacknowledged messages. Do that while the proxy is still up.
    gc.collect()
    with redis.Redis(host=REDIS_HOST, port=REDIS_PORT) as client:
        for key in client.scan_iter(match=f"{key_prefix}*"):
            client.delete(key)


@pytest.mark.filterwarnings("ignore::celery.exceptions.CPendingDeprecationWarning")
def test_worker_survives_broker_outage(worker, broker_proxy, caplog):
    """
    A worker holding an unacknowledged task reconnects after losing the broker.

    With celery 5.7.0 and kombu 5.7.0 the worker exits instead, see
    https://github.com/celery/celery/issues/11054.
    """
    worker.send("tests.block")
    assert worker.started.wait(timeout=10)

    broker_proxy.outage()
    assert broker_proxy.refused.wait(timeout=10)

    worker.send("tests.marker")
    # One pool thread: the marker task runs once the blocked one finishes.
    worker.release.set()
    assert worker.ran.wait(timeout=20), [
        record.getMessage()
        for record in caplog.records
        if record.levelno >= logging.CRITICAL
    ]
