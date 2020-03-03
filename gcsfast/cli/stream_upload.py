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
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed)
from logging import getLogger
from time import time
from typing import List, Iterable
from sys import stdin

from google.cloud import storage

LOG = getLogger(__name__)


def stream_upload_command(threads: int, slice_size: int, object_path: str,
                          file_path: str) -> None:
    gcs = storage.Client()

    input_stream = stdin.buffer
    if file_path:
        input_stream = open(file_path, "rb")

    upload_slice_size = slice_size

    executor = ThreadPoolExecutor(max_workers=threads)

    LOG.info("Reading input")
    start_time = time()
    futures = []
    read_bytes = 0
    slice_number = 0
    while not input_stream.closed:
        slice_bytes = input_stream.read(upload_slice_size)
        read_bytes += len(slice_bytes)  
        if slice_bytes:
            LOG.debug("Read a slice")
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

    LOG.info("Composing")
    final_blob = storage.Blob.from_string(object_path)
    final_blob.upload_from_file(io.BytesIO(b''), client=gcs)

    for composition in composition_steps(slices):
        composition.insert(0, final_blob)
        LOG.debug("Composing: {}".format([blob.name for blob in composition]))
        final_blob.compose(composition, client=gcs)

    LOG.info("Cleanup")
    for blob in slices:
        blob.delete(client=gcs)

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
    return blob


def composition_steps(slices: List) -> Iterable[List]:
    while len(slices):
        chunk = slices[:31]
        yield chunk
        slices = slices[31:]


def b_to_mb(byts: int):
    return round(byts / 1000 / 1000, 1)