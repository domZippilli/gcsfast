# gcsfast

Experimental fast file transfer for Google Cloud Storage.

## Status

This software is offered on an _"AS IS", EXPERIMENTAL_ basis, and only guaranteed to
demonstrate concepts -- NOT to act as production data transfer software. Any
and all usage of it is at your sole discretion. Any costs or damages resulting
from its use are the sole responsibility of the user. You are advised to read
and understand all source code in this software before using it for any reason.

---

## Step-by-step installation for Ubuntu 18.04 LTS in GCP

This installation will add `gcsfast` to Ubuntu 18.04's system Python, and will be
available as an executable on default user paths.

1. Create a GCE virtual machine with the Ubuntu 18.04 LTS image, and SSH into it.
2. Install Python 3.8 and update pip:

```shell
sudo apt update
sudo apt install python3-pip python3.8
python3.8 -m pip install -U pip
```

3. Install gcsfast. Use the `-e` flag to make an "editable" install,
   such that you can modify the files in the repo (or pull the latest
   modifications) and run the executable with those modifications, without
   reinstalling. You can omit this for non-development installations.

```shell
git clone https://github.com/domZippilli/gcsfast.git
cd gcsfast
python3.8 -m pip install -e .
```

4. [Temporary] You need the 5.7.0 version of gcloud-aio-storage, which has
   changes required by gcsfast. This hasn't been released yet, so clone the
   repository and install from the latest source to overwrite the installed
   version:

```shell
git clone https://github.com/talkiq/gcloud-aio.git
cd gcloud-aio/storage
python3.8 -m pip install .
```

5. You will probably get a warning like:
   `WARNING: The script gcsfast is installed in '/home/$USER/.local/bin' which is not on PATH.` To fix this, simply modify your path:

```shell
echo PATH=/home/$USER/.local/bin:$PATH >> ~/.bashrc && source ~/.bashrc
```

---

## Usage

```
Usage: gcsfast [OPTIONS] COMMAND [ARGS]...

  GCS fast file transfer tool.

Options:
  -l, --log_level TEXT  Set log level.
  --help                Show this message and exit.

Commands:
  download  Asyncio-based file download from GCS.
  upload    Stream data of an arbitrary length into an object in GCS.
```

gcsfast is designed to perform well in large file transfers (.5GB or greater).

Smaller files are supported, but currently not detected and optimized. As such,
small file transfers may be a bit slower or use more operations than expected.

### Upload

For each given file, gcsfast will get the file size, and upload "slices" of the
file into _separate objects_ simultaneously, and then use the `compose`
operation to create a single object. Therefore, per file, operations costs will
be significantly more than a single upload operation.

By default, upload slices will equal your CPU count as reported by the Python
`multiprocessing` module. So a typical upload will look like this, as far as
GCS operations:

- 1 Class A \* slices
- 1 Class A to compose the slices into the final object

In some cases where there are more than 32 slices, the number of compositions
may be higher. For example, if there are 64 slices, there will be three; one to
compose slices [1-32], one to compose slices [33-64], and one to compose the
previous two intermediate slices into one.

Note that these operations costs are very similar to those incurred by `gsutil`.

Upload only supports creating objects in the Standard storage class. You cannot
compose across storage classes, so you must do a final
[rewrite](https://cloud.google.com/storage/docs/changing-storage-classes#gsutil)
operation to the final storage class.

### Download

For each given object, gcsfast will get the object size, and download "slices"
of the object simultaneously into a single file on the local filesystem. This
requires a filesystem which supports sparse files, which almost all do.

As with upload, operations costs can be higher. For each file, you can expect:

- 1 class B operation \* slices

Again, these operations costs are very similar to those incurred by `gsutil`.

---

## Examples

#### Download an object to local file

`gcsfast download gs://mybucket/myblob destination_file`

#### Upload from stdin

`gcsfast upload - gs://mybucket/mystream`

#### Upload from file/FIFO

`gcsfast upload myfile gs://mybucket/myfile`

---

## Benchmarks

`gcsfast` compares favorably with `gsutil`. Default tunings for `gcsfast` are
far more aggressive, but that's cheating. So, the following are results with
`gsutil` well-optimized.

Note that `gcsfast` doesn't perform checksumming in any cases. This may account
for some of the performance differences. These benchmarks were validated with
checksums after-the-fact, however.

### Benchmark setup

- us-west1 for both VM and bucket
- n1-standard-32 (32 vCPUs, 120 GB memory), Skylake
- 4x375GB NVME in RAID0

---

### Download (1 x 60GiB)

Before tests, the first download is completed and discarded to ensure caching
is not a factor.

#### gsutil to local SSD

```shell
time gsutil -m \
  -o"GSUtil:sliced_object_download_threshold=1" \
  -o"GSUtil:sliced_object_download_max_components=96" \
  -o"GSUtil:parallel_process_count=32" \
  -o"GSUtil:parallel_thread_count=3" \
  cp gs://testbucket/testimage1 ./testimage1
```

- **Result times**: 1:36, 1:42, 1:42
- **Result goodput**: 5.15Gbps

#### gcsfast to local SSD

```shell
time gcsfast download gs://testbucket/testimage1 ./testimage1
```

- **Result times**: 1:04, 1:02, 1:06
- **Result goodput**: 8.05Gbps

#### gsutil to tmpfs

The same as above, but using tmpfs RAM disk as the destination.

- **Result times**: 1:27, 1:25, 1:25
- **Result goodput**: 6.01Gbps

#### gcsfast to tmpfs

The same as above, but using tmpfs RAM disk as the destination.

- **Result times**: 0:43, 0:43, 0:44
- **Result goodput**: 11.89Gbps

#### Analysis

gcsfast is **56% faster** to local SSD RAID, with a goodput gain of 2.9Gbps.

gcsfast is **98% faster** to RAM disk, with a goodput gain of 5.88Gbps.

These data indicate gcsfast is likely to take more advantage of faster
writing devices, like high-performance filers.

---

### Download (10 x 2.5GiB)

Before tests, the first download is completed and discarded to ensure caching
is not a factor.

#### gsutil to local SSD

```shell
time gsutil -m \
  -o"GSUtil:sliced_object_download_threshold=1" \
  -o"GSUtil:sliced_object_download_max_components=96" \
  -o"GSUtil:parallel_process_count=32" \
  -o"GSUtil:parallel_thread_count=3" \
  cp gs://testbucket/images* ./
```

- **Result times**: 0:36, 0:37, 0:36
- **Result goodput**: 5.91Gbps

#### gcsfast to local SSD

```shell
time gcsfast download $(gsutil ls gs://testbucket/images*) .
```

- **Result times**: 0:21, 0:22, 0:21
- **Result goodput**: 10.07Gbps

#### Analysis

gcsfast is **70% faster**, with a goodput gain of 4.16Gbps.

---

### Upload (1 x 60GiB file)

#### gsutil from local SSD

```shell
time gsutil -m \
  -o"GSUtil:parallel_composite_upload_threshold=1" \
  -o"GSUtil:parallel_process_count=32" \
  -o"GSUtil:parallel_thread_count=3" \
  cp ./testimage1 gs://testbucket/testimage1
```

- **Result times**: 1:23, 1:24, 1:22
- **Result goodput**: 6.21Gbps

#### gcsfast from local SSD

```shell
time gcsfast upload testimage1 gs://testbucket/testimage1
```

- **Result times**: 1:08, 1:08, 1:08
- **Result goodput**: 7.58Gbps

#### Analysis

gcsfast is **22% faster**, with a goodput gain of 1.37Gbps.

These tests were repeated with tmpfs, with no change.

---

### Upload (1 x 2.5GiB file)

#### gsutil from local SSD

```shell
time gsutil -m \
  -o"GSUtil:parallel_composite_upload_threshold=1" \
  -o"GSUtil:parallel_process_count=32" \
  -o"GSUtil:parallel_thread_count=3" \
  cp ./image1 gs://testbucket/image1
```

- **Result times**: 0:06.5, 0:06.4, 0:06.3
- **Result goodput**: 3.36Gbps

#### gcsfast from local SSD

```shell
time gcsfast upload image1 gs://testbucket/image1
```

- **Result times**: 0:03.0, 0:03.0, 0:03.0
- **Result goodput**: 7.16Gbps

#### Analysis

gcsfast is **113% faster**, with a goodput gain of 3.8Gbps.

## Copyright and License

Copyright 2021 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
