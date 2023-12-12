# -*- coding: utf-8 -*-
# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Conveyor filterer is a daemon to manage rucio transfer batching.
"""
import logging
import threading
import traceback
from collections import defaultdict
from types import FrameType
from typing import TYPE_CHECKING, Optional

import math
from sqlalchemy import null

import rucio.db.sqla.util
from rucio.common.config import config_get
from rucio.common.stopwatch import Stopwatch
from rucio.common import exception
from rucio.common.logging import setup_logging
from rucio.core.monitor import MetricManager
from rucio.core.request import (get_request_stats, release_all_waiting_requests, release_waiting_requests_fifo,
                                release_waiting_requests_grouped_fifo, set_transfer_limit_stats, re_sync_all_transfer_limits,
                                reset_stale_waiting_requests,
                                get_batched_request_groups_by_did, filter_request_groups)
from rucio.core.rse import RseCollection
from rucio.core.transfer import applicable_rse_transfer_limits
from rucio.daemons.common import db_workqueue, ProducerConsumerDaemon
from rucio.db.sqla.constants import RequestState, TransferLimitDirection

if TYPE_CHECKING:
    from rucio.daemons.common import HeartbeatHandler

GRACEFUL_STOP = threading.Event()
METRICS = MetricManager(module=__name__)
DAEMON_NAME = 'conveyor-filterer'


def filterer(
        once=False,
        sleep_time=10,
        partition_wait_time=10,
        total_threads=1,
):
    """
    Main loop to check for requests ready to be batch-filtered.
    """

    logging.info('Filterer starting')
    partition_hash_var = config_get('conveyor', 'partition_hash_var', default=None, raise_exception=False)

    @db_workqueue(
        once=once,
        graceful_stop=GRACEFUL_STOP,
        executable=DAEMON_NAME,
        partition_wait_time=partition_wait_time,
        sleep_time=sleep_time)
    def _db_producer(*, activity: str, heartbeat_handler: "HeartbeatHandler"):
        request_groups = _fetch_requests(
            heartbeat_handler,
            partition_hash_var=partition_hash_var
        )
        return True, request_groups

    def _consumer(request_groups):
        if request_groups is None:
            return
        logger = logging.log
        logger(logging.INFO, "Filterer - schedule batches")
        try:
            _handle_requests(request_groups, logger=logger)
        except Exception:
            logger(logging.CRITICAL, "Failed to schedule batches, error: %s" % (traceback.format_exc()))

    ProducerConsumerDaemon(
        producers=[_db_producer],
        consumers=[_consumer],
        # consumers=[_consumer for _ in range(total_threads)],
        graceful_stop=GRACEFUL_STOP,
    ).run()


def stop(signum: Optional[int] = None, frame: Optional[FrameType] = None) -> None:
    """
    Graceful exit.
    """

    GRACEFUL_STOP.set()


def run(once=False, sleep_time=10, threads: int=1):
    """
    Starts up the conveyer threads.
    """
    setup_logging(process_name=DAEMON_NAME)

    if rucio.db.sqla.util.is_old_db():
        raise exception.DatabaseException('Database was not updated, daemon won\'t start')

    filterer(once=once, sleep_time=sleep_time)


def _fetch_requests(
        heartbeat_handler: "HeartbeatHandler",
        partition_hash_var: Optional[str],
):
    """
    Fetches all requests of unique DIDs in the BATCH_FILTERING state.
    """
    worker_number, total_workers, logger = heartbeat_handler.live()

    stopwatch = Stopwatch()
    request_groups = get_batched_request_groups_by_did(
        total_workers,
        worker_number,
        partition_hash_var
    )
    stopwatch.stop()
    logger(logging.INFO, 'Got %s batches for batch filtering in %s seconds',
           len(request_groups.items()),
           stopwatch.elapsed)
    return request_groups

def _handle_requests(
    request_groups,
    logger,
):
    """
    Release batches of requests to the waiting state
    """
    stopwatch = Stopwatch()
    filter_request_groups(request_groups)
    stopwatch.stop()
    logger(logging.INFO, 'Request release computation completed in %s seconds',
           stopwatch.elapsed)