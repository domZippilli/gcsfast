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
from gcsfast.cli.upload import upload_command
from gcsfast.cli.upload_standard import upload_standard_command
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
@click.argument('file_args', nargs=-1, required=True)
def download(context: object, file_args: str) -> None:
    """
    Asyncio-based file download from GCS.

    FILE_ARGS is a sequence of either:\n
      (a) n GCS objects, followed by a directory. This will result in all
          the objects being downloaded to the directory.\n
      (b) n pairs of (object, file). This will result in each object being
          downloaded to the file it is paired with.
    """
    init(**context.obj)
    return download_command(file_args)


if __name__ == "__main__":
    main()


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


# pylint: disable=too-many-arguments
@main.command()
@click.pass_context
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
def upload_standard(context: object, threads: int, slice_size: int,
                    io_buffer: int, object_path: str, file_path: str) -> None:
    """
    Stream data of an arbitrary length into Standard storage class only.

    This is the same as upload, but overrides the bucket default storage class
    to Standard in order to compose objects faster. Files uploaded this way can
    be rewritten to another storage class later.
    """
    init(**context.obj)
    return upload_standard_command(threads, slice_size, io_buffer, object_path,
                                   file_path)


if __name__ == "__main__":
    main()
