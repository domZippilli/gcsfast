# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Implementation of "stream_upload" command.
"""
import io
from logging import getLogger
from time import time, sleep
from typing import List, Iterable
from sys import stdin

from google.cloud import storage

from gcsfast.thread import BoundedThreadPoolExecutor

LOG = getLogger(__name__)


def stream_upload_command(no_compose: bool, threads: int, slice_size: int, object_path: str,
                          file_path: str) -> None:
    gcs = storage.Client()

    input_stream = stdin.buffer
    if file_path:
        input_stream = open(file_path, "rb")

    upload_slice_size = slice_size

    executor = BoundedThreadPoolExecutor(max_workers=threads, queue_size=threads+2)

    LOG.info("Reading input")
    start_time = time()
    futures = []
    read_bytes = 0
    slice_number = 0
    while not input_stream.closed:
        slice_bytes = input_stream.read(upload_slice_size)
        read_bytes += len(slice_bytes)  
        if slice_bytes:
            LOG.debug("Read slice {}, {} bytes".format(slice_number, read_bytes))
            slice_blob = executor.submit(
                upload_bytes, slice_bytes,
                object_path + "_slice{}".format(slice_number), gcs)
            futures.append(slice_blob)
            slice_number += 1
        else:
            LOG.info("EOF: {} bytes".format(read_bytes))
            break

    LOG.info("Waiting for uploads to finish")
    slices = []
    for slyce in futures:
        slices.append(slyce.result())

    transfer_time = time() - start_time

    if not no_compose:
        LOG.info("Composing")
        final_blob = storage.Blob.from_string(object_path)
        final_blob.upload_from_file(io.BytesIO(b''), client=gcs)

        for composition in composition_steps(slices):
            composition.insert(0, final_blob)
            LOG.debug("Composing: {}".format([blob.name for blob in composition]))
            final_blob.compose(composition, client=gcs)
            sleep(1) # can only modify object once per second

        LOG.info("Cleanup")
        for blob in slices:
            executor.submit(blob.delete, client=gcs)
            sleep(.005) # quick and dirty rate-limiting, sorry Dijkstra

    LOG.info("Done")
    LOG.info("Overall seconds elapsed: {}".format(time() - start_time))
    LOG.info("Bytes read: {}".format(read_bytes))
    LOG.info("Transfer time: {}".format(transfer_time))
    LOG.info("Transfer rate Mb/s: {}".format(
        b_to_mb(int(read_bytes / transfer_time)) * 8))


def upload_bytes(bites: bytes, target: str,
                 client: storage.Client = None) -> storage.Blob:
    client = client if client else storage.Client()
    slice_reader = io.BytesIO(bites)
    blob = storage.Blob.from_string(target)
    blob.upload_from_file(slice_reader, client=client)
    LOG.info("Completed upload of: {}".format(blob.name))
    return blob


def composition_steps(slices: List) -> Iterable[List]:
    while len(slices):
        chunk = slices[:31]
        yield chunk
        slices = slices[31:]


def b_to_mb(byts: int):
    return round(byts / 1000 / 1000, 1)