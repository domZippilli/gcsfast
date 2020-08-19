# Copyright 2019 Google LLC
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
Custom GCS utility code.
"""
from logging import getLogger
from typing import Dict

from google.cloud import storage

from gcsfast.libraries.tunables import Tunables
from gcsfast.libraries.utils import b_to_mb

LOG = getLogger(__name__)


def tokenize_gcs_url(url: str) -> Dict[str, str]:
    try:
        protocol, remaining = url.split("://")
        bucket, path = remaining.split("/", 1)
        filename = path.split("/")[-1]
        return {
            "url": url,
            "protocol": protocol,
            "bucket": bucket,
            "path": path,
            "filename": filename
        }
    except Exception:
        LOG.error("Can't parse GCS URL: {}".format(url))
        exit(1)


def get_gcs_client() -> storage.Client:
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


def calculate_slice_size(blob_size: int, tuning: Tunables) -> int:
    """Calculate the appropriate slice size for a given blob to be divided among
    some number of ranged download jobs.

    Arguments:
        blob_size {int} -- The overall blob size.
          for job runners that will subdivide the slice across threads.
        tuning {Tunables} -- An object with transfer tunable settings.

    Returns:
        int -- The slice size to use for the given number of jobs.
    """
    # Unpack tunables, calculate slice size bounds
    jobs, threads = tuning.process_count, tuning.thread_count
    min_slice_size = tuning.minimum_download_slice_size * threads
    max_slice_size = tuning.maximum_download_slice_size * threads
    LOG.info("Threads * Minimum slice size\t: %s MB", b_to_mb(min_slice_size))
    LOG.info("Threads * Maximum slice size\t: %s MB", b_to_mb(max_slice_size))

    # If the blob is smaller than the minimum slice, no slicing.
    if blob_size < min_slice_size:
        LOG.info("Blob smaller than minimum slice size; cannot slice.")
        return blob_size

    # Calculate the size of splitting the blob evenly, and if out of bounds,
    # pick the nearest bound.
    evenly_among_workers = int(blob_size / jobs)
    if evenly_among_workers < min_slice_size:
        LOG.info(
            "Blob will be sliced into minimum slice sizes; there will be "
            "fewer slices than workers. You may want to specify a smaller "
            "(minimum) slice size.")
        return min_slice_size
    if evenly_among_workers > max_slice_size:
        LOG.info(
            "Blob will be sliced into maximum slice sizes; there will be more"
            "slices than workers (this is OK as long as workers optimize "
            "throughput).")
        return max_slice_size
    LOG.info("Blob can be sliced evenly among workers.")
    return evenly_among_workers


class DownloadJob(dict):
    """Describes a download job.

    Arguments:
        dict {[type]} -- [description]

    Returns:
        [type] -- [description]
    """
    def __init__(self, url_tokens, start, end, slice_number, output_file):
        self["url_tokens"] = url_tokens
        self["start"] = start
        self["end"] = end
        self["slice_number"] = slice_number
        self["output_file"] = output_file

    def __str__(self):
        return super().__str__()

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__.update(d)
