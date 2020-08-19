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
import pickle
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed, wait)
from logging import getLogger
from multiprocessing import cpu_count
from pprint import pprint
from sys import stdin
from time import time
from typing import Dict, Iterable, List, Tuple

from google.cloud import storage

from gcsfast.libraries.gcs import (DownloadJob, calculate_slice_size, get_blob,
                                   get_bucket, get_gcs_client,
                                   tokenize_gcs_url)
from gcsfast.libraries.tunables import Tunables
from gcsfast.libraries.utils import b_to_mb, mkdir_for_file, subdivide_range

io.DEFAULT_BUFFER_SIZE = 128 * 2**10
TUNING = Tunables()
LOG = getLogger(__name__)


def download_many_command(processes: int,
                          threads: int,
                          input_lines: str,
                          input_list: List = None) -> None:
    # Override tunables
    if processes:
        TUNING.process_count = processes
    if threads:
        TUNING.thread_count = threads

    LOG.info("Tuning settings: %s", TUNING)

    # Get input lines
    lines = None
    if input_list:
        lines = input_list
    elif input_lines == "-":
        lines = stdin.readlines()
    else:
        lines = open(input_lines, "r").readlines()

    LOG.debug('Download lines: %s', lines)

    # Tokenize the lines
    tokenized = generate_tokenized_urls(lines)

    # Generate download jobs
    jobs = generate_download_jobs(tokenized)

    # Run jobs
    try:
        with ProcessPoolExecutor(max_workers=TUNING.process_count) as executor:
            if all(executor.map(run_download_job, jobs)):
                LOG.info("All done!")
            else:
                LOG.error("Something went wrong!")
    except KeyboardInterrupt:
        LOG.error("Ctrl-C caught, quitting.")
        exit(1)


def generate_tokenized_urls(lines: Iterable[str]) -> Iterable[Dict[str, str]]:
    for line in lines:
        line = line.strip()
        if line:
            source_part, dest_part = line.split(",")
            tokens = tokenize_gcs_url(source_part)
            # Default destination filepath to the object name
            dest_part = dest_part if dest_part else tokens["path"]
            LOG.debug("Yielding tokenized line: %s", (tokens, dest_part))
            yield (tokens, dest_part)


def generate_download_jobs(input_iter: Iterable[Tuple[Dict[str, str], str]]
                           ) -> Iterable[DownloadJob]:
    """
    Generate DownloadJobs necessary to completely download the blobs.

    This function serves mainly to generate the specific byte ranges that each
    job should target.
    
    Arguments:
        input_iter {(Dict[str, str], str)} -- (Tokenized GCS URL, destination path).
    
    Returns:
        Iterable[DownloadJob] -- A sequence of DownloadJob definitions that will get the
          entire blob in slices.
    """
    for url_tokens, output_file in input_iter:
        # Get the object metadata
        LOG.debug("Getting object metadata: %s", url_tokens)
        gcs = get_gcs_client()
        bucket = get_bucket(gcs, url_tokens)
        blob = get_blob(bucket, url_tokens)
        LOG.info("%s blob size\t\t: %s (%s MB)", url_tokens["url"], blob.size,
                 b_to_mb(blob.size))

        # Calculate the optimal slice size, within bounds
        slice_size = calculate_slice_size(blob.size, TUNING)
        LOG.info("%s final slice size\t: %s MB", url_tokens["url"],
                 b_to_mb(slice_size))

        # Form definitions of each download job
        jobs = define_jobs_to_get_file(url_tokens, output_file, slice_size,
                                       blob.size)
        LOG.info("%s slice count: %i", url_tokens["url"], len(jobs))

        for job in jobs:
            yield job
            LOG.debug("Yielded process-level job: %s", job)
    LOG.info("All jobs generated.")


def define_jobs_to_get_file(url_tokens: Dict[str, str], output_file: str,
                            slice_size: int,
                            blob_size: int) -> List[DownloadJob]:
    jobs = []
    slice_number = 1
    start = 0
    finish = -1
    while finish < blob_size:
        finish = start + slice_size
        jobs.append(
            DownloadJob(url_tokens, start, min(finish, blob_size),
                        slice_number, output_file))
        slice_number += 1
        start = finish + 1
    return jobs


def run_download_job(job: DownloadJob) -> bool:
    LOG.info("Slice #%i starting", job["slice_number"])

    # Get client and blob for this process.
    gcs = get_gcs_client()
    url_tokens = job.url_tokens
    bucket = get_bucket(gcs, url_tokens)
    blob = get_blob(bucket, url_tokens)

    # Set blob transfer chunk size.
    blob.chunk_size = TUNING.transfer_chunk_size

    # Retrieve remaining job details.
    start = job.start
    end = job.end

    # set the output file and idempotently mkdir
    output_filename = job.output_file
    mkdir_for_file(output_filename)

    # Inner function with references to blob above, for thread pooling
    def _download_range(start_and_end: tuple):
        LOG.debug("Starting download job for bytes %s", start_and_end)
        s, e = start_and_end
        with open(output_filename, "wb") as output:
            output.seek(s)
            blob.download_to_file(output, start=s, end=e)
        return True

    # Sub-slice the download over all the local threads.
    start_time = time()
    with ThreadPoolExecutor(max_workers=TUNING.thread_count) as executor:
        ranges = subdivide_range(start, end, TUNING.thread_count)
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
