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

LOG = getLogger(__name__)


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
