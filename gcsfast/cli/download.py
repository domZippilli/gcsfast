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
import asyncio
from logging import getLogger
from multiprocessing import cpu_count
from os import path
from typing import List, Tuple

import aiohttp
from aiomultiprocess import Pool
from contexttimer import Timer
from gcloud.aio.storage import Storage

from gcsfast.libraries.utils import group_n, b_to_mb, subdivide_range

LOG = getLogger(__name__)


class DownloadJob(dict):
    def __init__(self, bucket, blob, output, start, end, elapsed=None):
        self["bucket"] = bucket
        self["blob"] = blob
        self["output"] = output
        self["start"] = start
        self["end"] = end
        self["elapsed"] = elapsed

    def __str__(self):
        return super().__str__()

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__.update(d)


class ConcurrencySettings(dict):
    def __init__(self, processes=None):
        if processes:
            self["processes"] = processes
        else:
            self["processes"] = cpu_count()

    def __str__(self):
        return super().__str__()

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__.update(d)


concurrency_settings = ConcurrencySettings()


def download_command(concurrency_multiple: float, file_args: str) -> None:
    """Downloads a single file.

    Arguments:
        object_path {str} -- The path to the GCS object.
        output_file {str} -- The path to the output file.
    """
    concurrency_settings["processes"] = round(concurrency_settings.processes *
                                              concurrency_multiple)

    # strip gs://, it's implied
    file_args = [x.replace("gs://", "") for x in file_args]

    # look at last item; is it a file or a directory?
    last_item = file_args[-1]

    # if a directory, assume we are getting n blobs to put in one place
    if path.isdir(last_item):
        LOG.info("Last item is a directory; parsing args as n sources to "
                 "place in the directory.")
        # construct the pairs
        pairs = []
        for blob in file_args[:-1]:
            dest = last_item + "/" + blob.rpartition("/")[2]
            pairs.append((blob, dest))
        asyncio.run(download_objects(pairs))

    # if /dev/null, assume we are getting n blobs for diagnostic purposes
    elif last_item == "/dev/null":
        asyncio.run(
            download_objects([(blob, "/dev/null") for blob in file_args[:-1]]))

    # assume we are getting pairs of object / filedest
    else:
        if len(file_args) % 2:
            raise Exception("Odd number of arguments, cannot construct pairs.")
        pairs = list(group_n(2, file_args))
        asyncio.run(download_objects(pairs))


async def download_objects(source_dest_pairs: List[Tuple[str, str]]):
    with Timer() as t:
        # Get object metadata and plan the downloads
        downloads = await describe_downloads(source_dest_pairs)
        # Log out the downloads we will do
        for download in downloads:
            LOG.info("Downloading: %s to %s, range %s-%s",
                     "/".join([download.bucket, download.blob]),
                     download.output, download.start, download.end)
        overall_bytes = 0
        # Send the downloads into an asyncio pool
        async with Pool(processes=concurrency_settings.processes) as pool:
            async for job in pool.map(do_download, downloads):
                job_bytes = (int(job.end) - int(job.start))
                overall_bytes += job_bytes
                bytes_ps = job_bytes / job.elapsed
                mbytes_ps = b_to_mb(bytes_ps)
                mbits_ps = mbytes_ps * 8
                LOG.info(
                    "Completed job: %s\n\t"
                    "Job download rate: %s MB/s; "
                    "%sMbps", job, mbytes_ps, mbits_ps)
        overall_time = t.elapsed
        LOG.info(
            "Overall elapsed time %.1fs\n\t"
            "Overall bytes copied %sMB\n\t"
            "Approximate transfer rate %sMB/s", overall_time,
            b_to_mb(overall_bytes), b_to_mb(overall_bytes / overall_time))


async def describe_downloads(
        source_dest_pairs: List[Tuple[str, str]]) -> List[DownloadJob]:
    downloads = list()

    # Get the metadata for all blobs to download
    async with aiohttp.ClientSession() as session:
        client = Storage(session=session)
        sources = await asyncio.gather(*[
            client.download_metadata(*source.split('/', 1))
            for source, _ in source_dest_pairs
        ])
        dests = [d for _, d in source_dest_pairs]
        source_dest_pairs = zip(sources, dests)

    # Construct the download jobs
    # TODO: Smarter subdivision.
    for pair in source_dest_pairs:
        source, dest = pair
        source_size = int(source["size"])
        for start, end in subdivide_range(0, source_size,
                                          concurrency_settings.processes):
            downloads.append(
                DownloadJob(source["bucket"], source["name"], dest, start,
                            end))

    return downloads


async def do_download(job) -> DownloadJob:
    async with aiohttp.ClientSession() as session:
        client = Storage(session=session)
        with Timer() as t:
            with open(job.output, mode='wb') as f:
                f.seek(job.start)
                headers = {"Range": f"bytes={job.start}-{job.end}"}
                # TODO: This is putting the whole download in memory.
                # Partially, this problem will be solved by using slices,
                # but proper buffered async writing would be nice
                f.write(await client.download(job.bucket,
                                              job.blob,
                                              headers=headers,
                                              timeout=60))
            job.elapsed = t.elapsed
            return job
