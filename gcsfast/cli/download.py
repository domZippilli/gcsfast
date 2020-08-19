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
Implementation of "download" command. This is a wrapper around "download_many."
"""
from logging import getLogger
from gcsfast.cli.download_many import download_many_command

LOG = getLogger(__name__)


def download_command(processes: int, threads: int, object_path: str,
                     output_file: str) -> None:
    """Downloads a single file using the download_many command.
    
    Arguments:
        processes {int} -- The number of processes to use.
        threads {int} -- The number of threads to have within each process.
          The chunks sent to processes are subdivided among threads.
        object_path {str} -- The path to the GCS object.
        output_file {str} -- The path to the output file.
    """
    LOG.info("Downloading %s to %s", object_path, output_file)
    return download_many_command(processes, threads, None,
                                 [','.join([object_path, output_file])])
