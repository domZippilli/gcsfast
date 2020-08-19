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
Container for transfer tunables.
"""
from multiprocessing import cpu_count

DEFAULT_MINIMUM_DOWNLOAD_SLICE_SIZE = 262144 * 4 * 64  # 64MiB
DEFAULT_MAXIMUM_DOWNLOAD_SLICE_SIZE = 262144 * 4 * 1024  # 1GiB
DEFAULT_TRANSFER_CHUNK_SIZE = 262144 * 4 * 16  # 16MiB
DEFAULT_PROCESS_COUNT = cpu_count()  # one per core
DEFAULT_THREAD_COUNT = 3  # two per process


class Tunables(dict):
    def __init__(self,
                 process_count=None,
                 thread_count=None,
                 transfer_chunk_size=None,
                 minimum_download_slice_size=None,
                 maximum_download_slice_size=None):
        super().__init__()
        # How many processes to use to parallelize the work
        self.setdefault(
            "process_count",
            process_count if process_count else DEFAULT_PROCESS_COUNT)
        # How many threads within those processes to use
        self.setdefault("thread_count",
                        thread_count if thread_count else DEFAULT_THREAD_COUNT)
        # How much data to send for chunked transfer encoded uploads
        self.setdefault(
            "transfer_chunk_size", transfer_chunk_size
            if transfer_chunk_size else DEFAULT_TRANSFER_CHUNK_SIZE)
        # Minimum size of a download slice
        self.setdefault(
            "minimum_download_slice_size",
            minimum_download_slice_size if minimum_download_slice_size else
            DEFAULT_MINIMUM_DOWNLOAD_SLICE_SIZE)
        # Maximum size of a download slice
        self.setdefault(
            "maximum_download_slice_size",
            maximum_download_slice_size if maximum_download_slice_size else
            DEFAULT_MAXIMUM_DOWNLOAD_SLICE_SIZE)

    @staticmethod
    def from_dict(settings):
        return {**Tunables(), **settings}

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__.update(d)
