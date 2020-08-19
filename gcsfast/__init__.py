#!/usr/bin/env python3
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
gcsfast main entry point.
"""
import logging
import warnings
from multiprocessing import cpu_count

import click

from gcsfast.cli.download import download_command
from gcsfast.cli.download_many import download_many_command
from gcsfast.cli.upload import upload_command
from gcsfast.libraries.utils import set_program_log_level

warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials")

logging.basicConfig()
LOG = logging.getLogger(__name__)


@click.group()
@click.option("-l",
              "--log_level",
              required=False,
              help="Set log level.",
              default=None)
@click.pass_context
def main(context: object = object(), **kwargs) -> None:
    """
    GCS fast file transfer tool.
    """
    context.obj = kwargs


def init(log_level: str = None) -> None:
    """
    Top-level initialization.

    Keyword Arguments:
        log_level {str} -- Desired log level. (default: {None})
    """
    set_program_log_level(log_level)


@main.command()
@click.pass_context
@click.option(
    "-p",
    "--processes",
    required=False,
    help="Set number of processes for simultaneous downloads. Default is "
    "multiprocessing.cpu_count().",
    type=int)
@click.option(
    "-t",
    "--threads",
    required=False,
    help="Set number of threads (per process) for simultaneous downloads. "
    "Default is 4. Default slice limits will be multiplied by this value (as "
    "slices are subdivided into threads).",
    type=int)
@click.argument('object_path')
@click.argument('file_path', type=click.Path(), required=False)
def download(context: object, processes: int, threads: int, object_path: str,
             file_path: str) -> None:
    """
    Download a GCS object as fast as possible.

    Your operating system must support sparse files; numerous slices will be
    written into specific byte offsets in the file at once, until they finally
    form a single contiguous file.

    OBJECT_PATH is the path to the object (use gs:// protocol).\n
    FILE_PATH is the filesystem path for the downloaded object.
    """
    init(**context.obj)
    return download_command(processes, threads, object_path, file_path)


if __name__ == "__main__":
    main()


@main.command()
@click.pass_context
@click.option(
    "-p",
    "--processes",
    required=False,
    help="Set number of processes for simultaneous downloads. Default is "
    "multiprocessing.cpu_count().",
    type=int)
@click.option(
    "-t",
    "--threads",
    required=False,
    help="Set number of threads (per process) for simultaneous downloads. "
    "Default is 4. Default slice limits will be multiplied by this value (as "
    "slices are subdivided into threads).",
    type=int)
@click.argument('input_lines')
def download_many(context: object, processes: int, threads: int,
                  input_lines: str) -> None:
    """
    Download a stream of GCS object URLs as fast as possible.

    Your operating system must support sparse files; numerous slices will be
    written into specific byte offsets in the file at once, until they finally
    form a single contiguous file.

    The incoming stream should be line delimited full GCS object URLs,
    like this:

      gs://bucket/object1
      gs://bucket/object2

    The objects will be placed in $PWD according to their "filename," that is
    the last string when the URL is split by forward slashes
    (i.e., gs://bucket/folder/object -> ./object).

    OBJECT_PATH is a file or stdin (-) from which to read full GCS object URLs,
    line delimited.
    """
    init(**context.obj)
    return download_many_command(processes, threads, input_lines)


# pylint: disable=too-many-arguments
@main.command()
@click.pass_context
@click.option("-n",
              "--no-compose",
              required=False,
              help="Do not compose the slices.",
              default=False,
              type=bool,
              is_flag=True)
@click.option(
    "-t",
    "--threads",
    required=False,
    help="Set number of threads for simultaneous slice uploads. Default is "
    "multiprocessing.cpu_count() * 4.",
    default=cpu_count() * 4,
    type=int)
@click.option(
    "-s",
    "--slice-size",
    required=False,
    help="Set the size of an upload slice. When this many bytes are read from "
    "stdin (before EOF), a new composite slice object upload will begin. "
    "Default is 16MB.",
    default=16 * 2**20,
    type=int)
@click.option(
    "-i",
    "--io_buffer",
    required=False,
    help="Set io.DEFAULT_BUFFER_SIZE, which determines the size of reads from "
    "disk, in bytes. Default is 128KB.",
    default=128 * 2**10,
    type=int)
@click.argument('object_path')
@click.argument('file_path', type=click.Path(), required=False)
def upload(context: object, no_compose: bool, threads: int, slice_size: int,
           io_buffer: int, object_path: str, file_path: str) -> None:
    """
    Stream data of an arbitrary length into an object in GCS.

    By default this command will read from stdin, but if a FILE_PATH is
    provided any file-like object (such as a FIFO) can be used.

    The stream will be read until slice-size or EOF is reached, at which point
    an upload will begin. Subsequent bytes read will be sent in similar slices
    until EOF. Finally, the slices will be composed into the target object.

    OBJECT_PATH is the path to the object (use gs:// protocol).
    FILE_PATH is the optional path for a file-like object.
    """
    init(**context.obj)
    return upload_command(no_compose, threads, slice_size, io_buffer,
                          object_path, file_path)


if __name__ == "__main__":
    main()
