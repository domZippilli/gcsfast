# Copyright 2021 Google LLC
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
Implementation of "upload_standard" command.
"""
import io
from concurrent.futures import Executor, Future
from itertools import count
from logging import getLogger
from os import stat
from sys import stdin
from time import sleep, time
from typing import Iterable, List, Any

from google.cloud import storage

from gcsfast.libraries.gcs import get_gcs_client
from gcsfast.libraries.thread import BoundedThreadPoolExecutor
from gcsfast.libraries.utils import b_to_mb

LOG = getLogger(__name__)

stats = {}


def upload_command(threads: int, slice_size: int, io_buffer: int,
                   file_path: str, object_path: str) -> None:
    """Upload a file-like into GCS using concurrent uploads. This is useful for
    inputs which can be read faster than a single TCP stream. Also, uploads
    from a device like a single spinning disk (where seek time is non-zero)
    may benefit from this operation as opposed to a sliced upload with multiple
    readers.

    Arguments:
        threads {int} -- The number of upload threads to use. The maximum
            amount of the stream that may be in memory is
            slice_size * threads * 2.5.
        slice_size {int} -- The slice size for each upload.
        io_buffer {int} -- The IO buffer size to use for file operations.
        file_path {str} -- (Optional) a file or file-like object to read.
            Defaults to stdin.
        object_path {str} -- The object path for the upload, or the prefix to
            use if composition is disabled.
    """
    # intialize
    io.DEFAULT_BUFFER_SIZE = io_buffer

    input_stream = stdin.buffer
    if file_path and file_path != "-":
        input_stream = open(file_path, "rb")

    upload_slice_size = slice_size if slice_size else max(
        [stat(file_path).st_size / threads, 16 * 2**20])

    executor = BoundedThreadPoolExecutor(max_workers=threads,
                                         queue_size=int(threads * 1.5))

    gcs = get_gcs_client()

    # start reading and uploading
    LOG.info("Reading input")
    start_time = time()
    futures = push_upload_jobs(input_stream, object_path, upload_slice_size,
                               gcs, executor)

    # wait for all uploads to finish and store the results
    slices = []
    for slyce in futures:
        slices.append(slyce.result())
    transfer_time = time() - start_time

    # compose
    compose(object_path, slices, gcs, executor)

    # cleanup and exit
    executor.shutdown(True)
    read_bytes = stats['read_bytes']
    LOG.info("Done")
    LOG.info("Overall seconds elapsed: {}".format(time() - start_time))
    LOG.info("Bytes read: {}".format(read_bytes))
    LOG.info("Transfer time: {}".format(transfer_time))
    LOG.info("Transfer rate Mb/s: {}".format(
        b_to_mb(int(read_bytes / transfer_time)) * 8))


def push_upload_jobs(input_stream: io.BufferedReader, object_path: str,
                     slice_size: int, client: storage.Client,
                     executor: Executor) -> List[Future]:
    """Given an input stream, perform a single-threaded, single-cursor read.
    This will be fanned out into multiple object slices, and optionally
    composed into a single object given as `object_path`. If composition is
    enabled, `object_path` will function as a prefix, to which the suffix
    `_sliceN` will be appended, where N is a monotonically increasing number
    starting with 1.

    Arguments:
        input_stream {io.BufferedReader} -- The input stream to read.
        object_path {str} -- The final object path or slice prefix to use.
        slice_size {int} -- The size of slice to target.
        client {storage.Client} -- The GCS client to use.
        executor {Executor} -- The executor to use for the concurrent slice
            uploads.

    Returns:
        List[Future] -- A list of the Future objects representing each blob
            slice upload. The result of each future will be of the type
            google.cloud.storage.Blob.
    """
    futures = []
    read_bytes = 0
    slice_number = 0
    while not input_stream.closed:
        slice_bytes = read_exactly(input_stream, slice_size)
        read_bytes += len(slice_bytes)
        stats['read_bytes'] = read_bytes
        if slice_bytes:
            LOG.debug("Read slice {}, {} bytes".format(slice_number,
                                                       read_bytes))
            slice_blob = executor.submit(
                upload_bytes, slice_bytes,
                object_path + "_slice{}".format(slice_number), client)
            futures.append(slice_blob)
            slice_number += 1
        else:
            LOG.info("EOF: {} bytes".format(read_bytes))
            break
    return futures


def read_exactly(input_stream: io.BufferedReader, length: int) -> bytes:
    """Read an exact amount of bytes from an input stream, unless EOF is reached.

    Arguments:
        input_stream {io.BufferedReader} -- The input stream to read from.
        length {int} -- The exact amount of bytes to read.

    Returns:
        bytes -- The bytes read. If zero and length is not zero, EOF.
    """
    accumulator = b''
    bytes_read = 0
    read_ops = 0
    while bytes_read < length:
        read_bytes = input_stream.read1(length - bytes_read)
        read_ops += 1
        bytes_read += len(read_bytes)
        accumulator += read_bytes
        if not len(read_bytes):
            break
    LOG.debug("Read exactly {} bytes in {} operations.".format(
        bytes_read, read_ops))
    return accumulator


def upload_bytes(bites: bytes,
                 target: str,
                 client: storage.Client = None) -> storage.Blob:
    """Upload a Python bytes object to a GCS blob.

    Arguments:
        bites {bytes} -- The bytes to upload.
        target {str} -- The blob to which to upload the bytes.

    Keyword Arguments:
        client {storage.Client} -- A client to use for the upload. If not
        provided, google.cloud.get_gcs_client() will be called.
        (default: {None})

    Returns:
        storage.Blob -- The uploaded blob.
    """
    client = client if client else get_gcs_client()
    slice_reader = io.BytesIO(bites)
    blob = storage.Blob.from_string(target)
    blob.storage_class = "STANDARD"
    LOG.debug("Starting upload of: {}".format(blob.name))
    blob.upload_from_file(slice_reader, client=client)
    LOG.info("Completed upload of: {}".format(blob.name))
    return blob


def compose(object_path: str, slices: List[storage.Blob],
            client: storage.Client, executor: Executor) -> storage.Blob:
    """Compose an object from an indefinite number of slices. Composition will
    be performed single-threaded but using a tree of accumulators to avoid the
    one second object update cooldown period in GCS. Cleanup will be performed
    concurrently using the provided executor.

    Arguments:
        object_path {str} -- The path for the final composed blob.
        slices {List[storage.Blob]} -- A list of the slices which should
            compose the blob, in order.
        client {storage.Client} -- A GCS client to use.
        executor {Executor} -- A concurrent.futures.Executor to use for
            cleanup execution.

    Returns:
        storage.Blob -- The composed blob.
    """
    LOG.info("Composing")

    chunks = generate_composition_chunks(slices)
    next_chunks = []
    identifier = generate_hex_sequence()

    while len(next_chunks) > 32 or not next_chunks:  # falsey empty list is ok
        for chunk in chunks:
            # make intermediate accumulator
            intermediate_accumulator = storage.Blob.from_string(
                object_path + next(identifier))
            LOG.info("Intermediate composition: %s", intermediate_accumulator)
            future_iacc = executor.submit(compose_and_cleanup,
                                          intermediate_accumulator, chunk,
                                          client, executor)
            # store reference for next iteration
            next_chunks.append(future_iacc)
        # let intermediate accumulators finish and go again
        chunks = generate_composition_chunks(next_chunks)

    # Now can do final compose
    final_blob = storage.Blob.from_string(object_path)
    final_chunk = [blob for sublist in chunks for blob in sublist]
    compose_and_cleanup(final_blob, final_chunk, client, executor)

    LOG.info("Composition complete")

    return final_blob


def ensure_results(maybe_futures: List[Any]) -> List[Any]:
    """Pass in a list that may contain Future, and if so, wait for
    the result of the Future and append it; for all other types in the list,
    simply append the value.

    Args:
        maybe_futures (List[Any]): A list which may contain Futures.

    Returns:
        List[Any]: A list with the values passed in, or Future.result() values.
    """
    results = []
    for mf in maybe_futures:
        if isinstance(mf, Future):
            results.append(mf.result())
        else:
            results.append(mf)
    return results


def compose_and_cleanup(blob: storage.Blob, chunk: List[storage.Blob],
                        client: storage.Client, executor: Executor):
    """Compose a blob and clean up its components. Cleanup tasks will be
    scheduled in the provided executor and the composed blob immediately
    returned.

    Args:
        blob (storage.Blob): The blob to be composed.
        chunk (List[storage.Blob]): The component blobs.
        client (storage.Client): A GCS client.
        executor (Executor): An executor in which to schedule cleanup tasks.

    Returns:
        storage.Blob: The composed blob.
    """
    # wait on results if the chunk is full of futures
    chunk = ensure_results(chunk)
    blob.compose(chunk, client=client)
    # cleanup components, no longer need them
    delete_objects_concurrent(chunk, executor, client)
    return blob


def delete_objects_concurrent(blobs, executor, client) -> None:
    """Delete GCS objects concurrently.

    Args:
        blobs ([type]): The objects to delete.
        executor ([type]): An executor to schedule the deletions in.
        client ([type]): GCS client to use.s
    """
    for blob in blobs:
        LOG.debug("Deleting slice {}".format(blob.name))
        executor.submit(blob.delete, client=client)
        sleep(.005)  # quick and dirty ramp-up, sorry Dijkstra


def generate_hex_sequence() -> Iterable[str]:
    """Generate an indefinite sequence of hexadecimal integers.

    Yields:
        Iterator[Iterable[str]]: The sequence of hex digits, as strings.
    """
    for i in count(0):
        yield hex(i)[2:]


def generate_composition_chunks(slices: List,
                                chunk_size: int = 32) -> Iterable[List]:
    """Given an indefinitely long list of blobs, return the list in 32 item chunks.

    Arguments:
        slices {List} -- A list of blobs which are slices of a desired final
            blob.

    Returns:
        Iterable[List] -- An iteration of 31-item chunks of the input list.

    Yields:
        Iterable[List] -- A 31-item chunk of the input list.
    """
    while len(slices):
        chunk = slices[:chunk_size]
        yield chunk
        slices = slices[chunk_size:]
