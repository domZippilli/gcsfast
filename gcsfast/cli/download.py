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
Implementation of "download" command.
"""
import io
import fileinput
from concurrent.futures import as_completed, wait, ProcessPoolExecutor, ThreadPoolExecutor
from logging import getLogger
from multiprocessing import cpu_count
from pprint import pprint
from time import time
from typing import Dict, List

from google.cloud import storage

from gcsfast.constants import (DEFAULT_MAXIMUM_DOWNLOAD_SLICE_SIZE,
                               DEFAULT_MINIMUM_DOWNLOAD_SLICE_SIZE)

# TODO move these to a dict or a class
io.DEFAULT_BUFFER_SIZE = 131072
PROCESS_COUNT = [cpu_count()]
THREAD_COUNT = [1]
TRANSFER_CHUNK_SIZE = [262144 * 4 * 16]
LOG = getLogger(__name__)


class DownloadJob(dict):
    def __init__(self, url_tokens, start, end, slice_number):
        self["url_tokens"] = url_tokens
        self["start"] = start
        self["end"] = end
        self["slice_number"] = slice_number

    def __str__(self):
        return super().__str__()


def download_command(processes: int, threads: int, io_buffer: int,
                     min_slice: int, max_slice: int, slice_size: int,
                     transfer_chunk: int, object_path: str,
                     output_file: str) -> None:
    # Set global tunables
    if io_buffer:
        io.DEFAULT_BUFFER_SIZE = io_buffer
    if transfer_chunk:
        TRANSFER_CHUNK_SIZE[0] = transfer_chunk
    if processes:
        PROCESS_COUNT[0] = processes
    if threads:
        THREAD_COUNT[0] = threads

    # Tokenize URL
    url_tokens = tokenize_gcs_url(object_path)

    # Override the output file if it's given
    if output_file:
        url_tokens["filename"] = output_file

    # Get processes
    workers = processes if processes else cpu_count()
    LOG.debug("Worker process count: %i", workers)
    LOG.debug("Threads per worker: %i", THREAD_COUNT[0])

    # Get the object metadata
    gcs = get_client()
    bucket = get_bucket(gcs, url_tokens)
    blob = get_blob(bucket, url_tokens)
    LOG.info("Blob size\t\t: {}".format(blob.size))

    # Calculate the optimal slice size, within bounds
    slice_size = slice_size if slice_size else calculate_slice_size(
        blob.size, workers, min_slice, max_slice, THREAD_COUNT[0])
    LOG.info("Final slice size\t: {}".format(slice_size))

    # Form definitions of each download job
    jobs = calculate_jobs(url_tokens, slice_size, blob.size)
    LOG.info("Slice count: %i", len(jobs))

    # Fan out the slice jobs
    with ProcessPoolExecutor(max_workers=workers) as executor:
        start_time = time()
        if all(executor.map(run_download_job, jobs)):
            elapsed = time() - start_time
            LOG.info(
                "Overall: %.1fs elapsed for %i MB download, %i Mbits per second.",
                elapsed, blob.size / 1000 / 1000,
                int((blob.size / elapsed) * 8 / 1000 / 1000))
        else:
            print("Something went wrong! Download again.")
            exit(1)

    # TODO: Final checksum


def run_download_job(job: DownloadJob) -> bool:
    # Get client and blob for this process.
    gcs = get_client()
    url_tokens = job["url_tokens"]
    bucket = get_bucket(gcs, url_tokens)
    blob = get_blob(bucket, url_tokens)
    # Set blob transfer chunk size.
    blob.chunk_size = TRANSFER_CHUNK_SIZE[0]
    # Retrieve remaining job details.
    start = job["start"]
    end = job["end"]
    output_filename = job["url_tokens"]["filename"]

    def _download_range(start_and_end: tuple):
        s, e = start_and_end
        with open(output_filename, "wb") as output:
            output.seek(s)
            blob.download_to_file(output, start=s, end=e)
        return True

    start_time = time()
    with ThreadPoolExecutor(max_workers=THREAD_COUNT[0]) as executor:
        ranges = subdivide_range(start, end, THREAD_COUNT[0])
        LOG.debug("Slice #%i: divided into ranges (per thread): %s",
                  job["slice_number"], ranges)
        # Perform download.
        if not all(executor.map(_download_range, ranges)):
            return False
    elapsed = time() - start_time

    # Log stats and return.
    bytes_downloaded = end - start
    LOG.info(
        "Slice #%i: %.1fs elapsed for %i MB download, %i Mbits per second",
        job["slice_number"], elapsed, bytes_downloaded / 1000 / 1000,
        int((bytes_downloaded / elapsed) * 8 / 1000 / 1000))
    return True


def subdivide_range(range_start, range_end, subdivisions: int) -> List[tuple]:
    range_size = range_end - range_start
    subrange_size = int(range_size / subdivisions)  # truncate the float
    ranges = []
    start = range_start
    finish = -1
    while finish < range_end:
        finish = start + subrange_size
        ranges.append((start, min(finish, range_end)))
        start = finish + 1
    return ranges


def calculate_jobs(url_tokens: Dict[str, str], slice_size: int,
                   blob_size: int) -> List[DownloadJob]:
    jobs = []
    slice_number = 1
    start = 0
    finish = -1
    while finish < blob_size:
        finish = start + slice_size
        jobs.append(
            DownloadJob(url_tokens, start, min(finish, blob_size),
                        slice_number))
        slice_number += 1
        start = finish + 1
    return jobs


def calculate_slice_size(blob_size: int, jobs: int, min_override: int,
                         max_override: int, multiplier: int) -> int:
    min_slice_size = min_override if min_override else DEFAULT_MINIMUM_DOWNLOAD_SLICE_SIZE * multiplier
    max_slice_size = max_override if max_override else DEFAULT_MAXIMUM_DOWNLOAD_SLICE_SIZE * multiplier
    LOG.info("Minimum slice size\t: {}".format(min_slice_size))
    LOG.info("Maximum slice size\t: {}".format(max_slice_size))
    if blob_size < min_slice_size:
        # No point in slicing.
        return blob_size
    evenly_among_workers = int(blob_size / jobs)
    if evenly_among_workers < min_slice_size:
        return min_slice_size
    if evenly_among_workers > max_slice_size:
        return max_slice_size
    return evenly_among_workers


def tokenize_gcs_url(url: str) -> Dict[str, str]:
    try:
        protocol, remaining = url.split("://")
        bucket, path = remaining.split("/", 1)
        filename = path.split("/")[-1]
        return {
            "protocol": protocol,
            "bucket": bucket,
            "path": path,
            "filename": filename
        }
    except Exception as e:
        LOG.error("Can't parse GCS URL: {}".format(url))
        exit(1)


def get_client() -> storage.Client:
    try:
        return storage.Client()
    except Exception as e:
        LOG.error("Error creating client: \n\t{}".format(e))
        exit(1)


def get_bucket(gcs: storage.Client, url_tokens: str) -> storage.Bucket:
    try:
        return gcs.get_bucket(url_tokens["bucket"])
    except Exception as e:
        LOG.error("Error accessing bucket: {}\n\t{}".format(
            url_tokens["bucket"], e))
        exit(1)


def get_blob(bucket: storage.Bucket, url_tokens: str) -> storage.Blob:
    try:
        return bucket.get_blob(url_tokens["path"])
    except Exception as e:
        LOG.error("Error accessing object: {}\n\t{}".format(
            url_tokens["path"], e))
