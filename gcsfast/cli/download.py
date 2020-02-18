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
from concurrent.futures import as_completed, wait, ProcessPoolExecutor
from logging import getLogger
from multiprocessing import cpu_count
from pprint import pprint
from time import time
from typing import Dict, List

from google.cloud import storage

from gcsfast.constants import (DEFAULT_MAXIMUM_DOWNLOAD_SLICE_SIZE,
                               DEFAULT_MINIMUM_DOWNLOAD_SLICE_SIZE)

io.DEFAULT_BUFFER_SIZE = 131072
LOG = getLogger(__name__)


class DownloadJob(dict):
    def __init__(self, url_tokens, start, end, slice_number):
        self["url_tokens"] = url_tokens
        self["start"] = start
        self["end"] = end
        self["slice_number"] = slice_number

    def __str__(self):
        return super().__str__()


def download_command(processes: int, object_path: str,
                     output_file: str) -> None:
    # Tokenize URL
    url_tokens = tokenize_gcs_url(object_path)

    # Override the output file if it's given
    if output_file:
        url_tokens["filename"] = output_file

    # Get parallelism
    workers = processes if processes else cpu_count()
    LOG.debug("Worker count: %i", workers)

    # Get the object metadata
    gcs = get_client()
    bucket = get_bucket(gcs, url_tokens)
    blob = get_blob(bucket, url_tokens)

    # Calculate the optimal slice size, within bounds
    slice_size = calculate_slice_size(blob.size, workers)
    LOG.info("Final slice size\t: {}".format(slice_size))

    # Form definitions of each download job
    jobs = calculate_jobs(url_tokens, slice_size, blob.size)

    # Fan out the slice jobs
    with ProcessPoolExecutor(max_workers=workers) as executor:
        start_time = time()
        if all(executor.map(run_download_job, jobs)):
            elapsed = time() - start_time
            LOG.info("%fs elapsed for %iMB download.", elapsed,
                     blob.size / 1000 / 1000)
            LOG.info("%i Mbits per second",
                     int((blob.size / elapsed) * 8 / 1000 / 1000))
        else:
            print("Something went wrong! Download again.")
            exit(1)

    # TODO: Final checksum


def run_download_job(job: DownloadJob) -> None:
    gcs = get_client()
    url_tokens = job["url_tokens"]
    bucket = get_bucket(gcs, url_tokens)
    blob = get_blob(bucket, url_tokens)
    blob.chunk_size = 262144 * 4 * 16
    start = job["start"]
    end = job["end"]
    output_filename = job["url_tokens"]["filename"]
    with open(output_filename, "wb") as output:
        output.seek(start)
        blob.download_to_file(output, start=start, end=end)
    return True


def calculate_jobs(url_tokens: Dict[str, str], slice_size: int,
                   blob_size: int) -> List[DownloadJob]:
    jobs = []
    slice_number = 0
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


def calculate_slice_size(blob_size: int, jobs: int) -> int:
    LOG.info("Blob size\t\t: {}".format(blob_size))
    LOG.info(
        "Minimum slice size\t: {}".format(DEFAULT_MINIMUM_DOWNLOAD_SLICE_SIZE))
    LOG.info(
        "Maximum slice size\t: {}".format(DEFAULT_MAXIMUM_DOWNLOAD_SLICE_SIZE))
    if blob_size < DEFAULT_MINIMUM_DOWNLOAD_SLICE_SIZE:
        # No point in slicing.
        return blob_size
    evenly_among_workers = int(blob_size / jobs)
    if evenly_among_workers < DEFAULT_MINIMUM_DOWNLOAD_SLICE_SIZE:
        return DEFAULT_MINIMUM_DOWNLOAD_SLICE_SIZE
    if evenly_among_workers > DEFAULT_MAXIMUM_DOWNLOAD_SLICE_SIZE:
        return DEFAULT_MAXIMUM_DOWNLOAD_SLICE_SIZE
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
