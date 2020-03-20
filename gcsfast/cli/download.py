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
from time import time
from typing import Dict, List, Iterable

from google.cloud import storage

from gcsfast.constants import (DEFAULT_MAXIMUM_DOWNLOAD_SLICE_SIZE,
                               DEFAULT_MINIMUM_DOWNLOAD_SLICE_SIZE)
from gcsfast.libraries.gcs import (get_blob, get_bucket, get_gcs_client,
                                   tokenize_gcs_url)
from gcsfast.libraries.utils import b_to_mb

TUNING = {}
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


def download_command(processes: int, threads: int, io_buffer: int,
                     min_slice: int, max_slice: int, slice_size: int,
                     transfer_chunk: int, object_path: str,
                     output_file: str) -> None:
    """Downloads a single file by breaking up the work across both processes and threads. This
    implementation is dependent on a filesystem that supports sparse files (which is most modern ones) as each
    download job will seek to the start of its slice in the file and write there.
    
    Arguments:
        processes {int} -- The number of processes to use.
        threads {int} -- The number of threads to have within each process.
          The chunks sent to processes are subdivided among threads.
        io_buffer {int} -- Size of the IO buffer to use.
        min_slice {int} -- Minimum download slice size.
        max_slice {int} -- Maximum download slice size.
        slice_size {int} -- Override slice size calculations and use this.
        transfer_chunk {int} -- Size of HTTP chunk to transfer from GCS.
        object_path {str} -- The path to the GCS object.
        output_file {str} -- The path to the output file.
    """
    # Set global tunables
    io.DEFAULT_BUFFER_SIZE = io_buffer
    TUNING["TRANSFER_CHUNK_SIZE"] = transfer_chunk
    TUNING["THREAD_COUNT"] = threads

    # Get processes
    workers = processes if processes else cpu_count()
    LOG.debug("Worker process count: %i", workers)
    LOG.debug("Threads per worker: %i", TUNING["THREAD_COUNT"])

    # Tokenize URL
    url_tokens = tokenize_gcs_url(object_path)

    # Override the output file if it's given
    # TODO: move this out of the tokens into the job definition
    if output_file:
        url_tokens["filename"] = output_file

    # Get the object metadata
    gcs = get_gcs_client()
    bucket = get_bucket(gcs, url_tokens)
    blob = get_blob(bucket, url_tokens)
    LOG.info("Blob size\t\t: {} ({} MB)".format(blob.size, b_to_mb(blob.size)))

    # Calculate the optimal slice size, within bounds
    slice_size = slice_size if slice_size else calculate_slice_size(
        blob.size, workers, min_slice, max_slice, TUNING["THREAD_COUNT"])
    LOG.info("Final slice size\t: {} MB".format(b_to_mb(slice_size)))

    # Form definitions of each download job
    jobs = generate_jobs(url_tokens, slice_size, blob.size)

    # Fan out the slice jobs
    with ProcessPoolExecutor(max_workers=workers) as executor:
        LOG.info("Beginning download of %s to %s...", object_path,
                 url_tokens["filename"])
        start_time = time()
        if all(executor.map(run_download_job, jobs)):
            elapsed = time() - start_time
            LOG.info(
                "Overall: %.1fs elapsed for %.1f MB download, %i Mbits per second.",
                elapsed, b_to_mb(blob.size),
                int((blob.size / elapsed) * 8 / 1000 / 1000))
        else:
            print("Something went wrong! Download again.")
            exit(1)


def run_download_job(job: DownloadJob) -> bool:
    """Run a download "job" as defined in a DownloadJob object.

    The job's download range will be subdivided among threads. If the
    thread argument is set to 1, this will have no effect.
    
    Arguments:
        job {DownloadJob} -- A DownloadJob object describing the blob range to download
          and a destination file.
    
    Returns:
        bool -- True if all threads completed successfully.
    """
    # Get client and blob for this process.
    gcs = get_gcs_client()
    url_tokens = job["url_tokens"]
    bucket = get_bucket(gcs, url_tokens)
    blob = get_blob(bucket, url_tokens)
    # Set blob transfer chunk size.
    blob.chunk_size = TUNING["TRANSFER_CHUNK_SIZE"]
    # Retrieve remaining job details.
    start = job["start"]
    end = job["end"]
    output_filename = job["url_tokens"]["filename"]

    start_time = time()
    with ThreadPoolExecutor(max_workers=TUNING["THREAD_COUNT"]) as executor:
        ranges = list(subdivide_range(start, end, TUNING["THREAD_COUNT"]))
        LOG.debug("Slice #%i: divided into ranges (per thread): %s",
                  job["slice_number"], ranges)
        # Partial application to prepare for map.
        downloader = lambda x: download_range(x, blob, output_filename)
        # Perform downloads.
        if not all(executor.map(downloader, ranges)):
            return False
    elapsed = time() - start_time

    # Log stats and return.
    bytes_downloaded = end - start
    LOG.info("Slice #%i: %.1fs elapsed for %i MB slice, %i Mbits per second",
             job["slice_number"], elapsed, b_to_mb(bytes_downloaded),
             int((bytes_downloaded / elapsed) * 8 / 1000 / 1000))
    return True


def download_range(start_and_end: tuple, blob: storage.Blob,
                   output_filename: str) -> bool:
    """Download a range of a blob into a file.
    
    Arguments:
        start_and_end {tuple} -- The start and end of the range.
        blob {storage.Blob} -- The blob to read from.
        output_filename {str} -- The file to write to.
    
    Returns:
        bool -- Success of the download.
    """
    s, e = start_and_end
    with open(output_filename, "wb") as output:
        output.seek(s)
        blob.download_to_file(output, start=s, end=e)
    return True


def subdivide_range(range_start, range_end,
                    subdivisions: int) -> Iterable[tuple]:
    """Generate n exclusive subdivisions of a numerical range.
    
    Arguments:
        range_start {[type]} -- The start of the range.
        range_end {[type]} -- The end of the range.
        subdivisions {int} -- The number of subdivisions.
    
    Returns:
        Iterable[tuple] -- A sequence of tuples (start, finish) for each
          subdivision.
    """
    range_size = range_end - range_start
    subrange_size = int(range_size / subdivisions)  # truncate the float
    start = range_start
    finish = -1
    while finish < range_end:
        finish = start + subrange_size
        yield (start, min(finish, range_end))
        start = finish + 1


def generate_jobs(url_tokens: Dict[str, str], slice_size: int,
                  blob_size: int) -> Iterable[DownloadJob]:
    """
    Generate DownloadJobs necessary to completely download the blob using
    the given slice size.

    This function serves mainly to generate the specific byte ranges that each
    job should target.
    
    Arguments:
        url_tokens {Dict[str, str]} -- Tokenized GCS URL.
        slice_size {int} -- The slice size to target. The final slice may be smaller.
        blob_size {int} -- The size of the blob, in bytes.
    
    Returns:
        Iterable[DownloadJob] -- A sequence of DownloadJob definitions that will get the
          entire blob in slices.
    """
    slice_number = 1
    start = 0
    finish = -1
    while finish < blob_size:
        finish = start + slice_size
        yield DownloadJob(url_tokens, start, min(finish, blob_size),
                          slice_number)
        slice_number += 1
        start = finish + 1


def calculate_slice_size(blob_size: int, jobs: int, min_override: int,
                         max_override: int, multiplier: int) -> int:
    """Calculate the appropriate slice size for a given blob to be divided among 
    some number of ranged download jobs.
    
    Arguments:
        blob_size {int} -- The overall blob size.
        jobs {int} -- The number of jobs to divide the blob into.
        min_override {int} -- A user-provided override value for the minimum slice size.
        max_override {int} -- A user-provided override value for the maximum slice size.
        multiplier {int} -- A multiplier for the min/max values. Use this to shift the slice sizes
          for job runners that will subdivide the slice across threads.
    
    Returns:
        int -- The slice size to use for the given number of jobs.
    """
    min_slice_size = min_override if min_override else DEFAULT_MINIMUM_DOWNLOAD_SLICE_SIZE * multiplier
    max_slice_size = max_override if max_override else DEFAULT_MAXIMUM_DOWNLOAD_SLICE_SIZE * multiplier
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
