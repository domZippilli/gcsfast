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
import fileinput
import io
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed, wait)
from logging import getLogger
from multiprocessing import cpu_count
from pprint import pprint
from sys import stdin
from time import time
from typing import Dict, List, Iterable

from google.cloud import storage

from gcsfast.constants import (DEFAULT_MAXIMUM_DOWNLOAD_SLICE_SIZE,
                               DEFAULT_MINIMUM_DOWNLOAD_SLICE_SIZE)
from gcsfast.libraries.gcs import get_gcs_client, get_bucket, get_blob, tokenize_gcs_url
from gcsfast.libraries.utils import b_to_mb

# TODO move tunables to a dict or a class
io.DEFAULT_BUFFER_SIZE = 131072
PROCESS_COUNT = [cpu_count()]
THREAD_COUNT = [1]
TRANSFER_CHUNK_SIZE = [262144 * 4 * 16]
LOG = getLogger(__name__)


class DownloadJob(dict):
    """Describes a download job. 
    
    Arguments:
        dict {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    def __init__(self, url_tokens, start, end, slice_number):
        self["url_tokens"] = url_tokens
        self["start"] = start
        self["end"] = end
        self["slice_number"] = slice_number

    def __str__(self):
        return super().__str__()


def download_many_command(processes: int, threads: int, io_buffer: int,
                      transfer_chunk: int, input_lines: str) -> None:
    # Set global tunables
    if io_buffer:
        io.DEFAULT_BUFFER_SIZE = io_buffer
    if transfer_chunk:
        TRANSFER_CHUNK_SIZE[0] = transfer_chunk
    if processes:
        PROCESS_COUNT[0] = processes
    if threads:
        THREAD_COUNT[0] = threads

    # Generate lines
    lines = None
    if input_lines == "-":
        lines = stdin.readlines()
    else:
        lines = open(input_lines, "r").readlines()

    # Generate tokenized lines
    tokenized = generate_tokenized_urls(lines)

    # Generate download jobs
    jobs = generate_download_jobs(tokenized)

    # Run jobs
    with ProcessPoolExecutor(max_workers=PROCESS_COUNT[0]) as executor:
        if all(executor.map(run_download_job, jobs)):
            LOG.info("All done!")
        else:
            LOG.error("Something went wrong! Download again.")


def generate_download_jobs(tokenized_urls: Iterable[Dict[str, str]]
                           ) -> Iterable[DownloadJob]:
    for url_tokens in tokenized_urls:
        # Get the object metadata
        gcs = get_gcs_client()
        bucket = get_bucket(gcs, url_tokens)
        blob = get_blob(bucket, url_tokens)
        LOG.info("%s blob size\t\t: %s (%s MB)", url_tokens["url"], blob.size,
                 b_to_mb(blob.size))

        # Calculate the optimal slice size, within bounds
        slice_size = calculate_slice_size(blob.size, PROCESS_COUNT[0],
                                          THREAD_COUNT[0])
        LOG.info("%s final slice size\t: %s MB", url_tokens["url"],
                 b_to_mb(slice_size))

        # Form definitions of each download job
        jobs = calculate_jobs(url_tokens, slice_size, blob.size)
        LOG.info("%s slice count: %i", url_tokens["url"], len(jobs))

        for job in jobs:
            yield job


def generate_tokenized_urls(lines: Iterable[str]) -> Iterable[Dict[str, str]]:
    for line in lines:
        line = line.strip()
        if line:
            yield tokenize_gcs_url(line)


def calculate_slice_size(blob_size: int, jobs: int, multiplier: int) -> int:
    min_slice_size = DEFAULT_MINIMUM_DOWNLOAD_SLICE_SIZE * multiplier
    max_slice_size = DEFAULT_MAXIMUM_DOWNLOAD_SLICE_SIZE * multiplier
    LOG.info("Minimum slice size\t: {} MB".format(b_to_mb(min_slice_size)))
    LOG.info("Maximum slice size\t: {} MB".format(b_to_mb(max_slice_size)))
    if blob_size < min_slice_size:
        LOG.info("Blob smaller than minimum slice size; cannot slice.")
        return blob_size
    evenly_among_workers = int(blob_size / jobs)
    if evenly_among_workers < min_slice_size:
        LOG.info(
            "Blob will be sliced into minimum slice sizes; there will be fewer slices than workers. You may want to specify a smaller (minimum) slice size."
        )
        return min_slice_size
    if evenly_among_workers > max_slice_size:
        LOG.info(
            "Blob will be sliced into maximum slice sizes; there will be more slices than workers (this is OK as long as workers optimize throughput)."
        )
        return max_slice_size
    LOG.info("Blob can be sliced evenly among workers.")
    return evenly_among_workers


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


def run_download_job(job: DownloadJob) -> bool:
    # Get client and blob for this process.
    gcs = get_gcs_client()
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
    LOG.info("Slice #%i: %.1fs elapsed for %i MB slice, %i Mbits per second",
             job["slice_number"], elapsed, b_to_mb(bytes_downloaded),
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