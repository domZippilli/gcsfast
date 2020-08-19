# gcsfast

Experimental fast file transfer for Google Cloud Storage.

## Installation

Clone this repo using `git clone ...`

`cd` into the repo and run `pip install .` or `pip install -e .` if you plan on making and running code edits.

## Usage

```
Usage: gcsfast [OPTIONS] COMMAND [ARGS]...

  GCS fast file transfer tool.

Options:
  -l, --log_level TEXT  Set log level.
  --help                Show this message and exit.

Commands:
  download       Download a GCS object as fast as possible.
  download-many  Download a stream of GCS objects as fast as possible.
  upload-stream  Stream data of an arbitrary length into an object in GCS.
```

See `--help` on each command for more info.

### Examples

*Download an object to local file*

`gcsfast -l DEBUG download gs://mybucket/myblob destination_file`

*Download a series of objects described in a file*

`gcsfast -l DEBUG download-many files.txt`

*Upload from stdin with a fixed slice size*

`gcsfast -l DEBUG upload gs://mybucket/mystream`

*Upload from file/FIFO with a fixed slice size*

`gcsfast -l DEBUG upload gs://mybucket/mystream myfile`

## Benchmarks

`gcsfast` compares favorably with `gsutil`. Default tunings for `gcsfast` are
far more aggressive, but that's cheating. Here are some results with `gsutil`
well-optimized.

Note that gcsfast doesn't perform checksumming in any cases. This may account
for some of the performance differences.

### Benchmark setup

us-west1 for both VM and bucket
n1-standard-32 (32 vCPUs, 120 GB memory), Skylake
4x375GB NVME in RAID0

### Download (1 x 60GiB)

Before tests, the first download is completed and discarded to ensure caching
is not a factor.

#### gsutil

```shell
time gsutil -m \
  -o"GSUtil:sliced_object_download_threshold=1" \
  -o"GSUtil:sliced_object_download_max_components=96" \
  -o"GSUtil:parallel_process_count=32" \
  -o"GSUtil:parallel_thread_count=3" \
  cp gs://testbucket/testimage1 ./testimage1
```

**Result times**: 1:28, 1:29, 1:28
**Result goodput**: 5.86Gbps

#### gcsfast

```shell
time gcsfast download gs://testbucket/testimage1 ./testimage1
```

**Result times**: 0:55, 0:55, 0:55
**Result goodput**: 9.37Gbps

#### Analysis

gcsfast is **60% faster**, with a goodput gain of 3.5Gbps.

### Download (10 x 2.5GiB)

Before tests, the first download is completed and discarded to ensure caching
is not a factor.

#### gsutil

```shell
time gsutil -m \
  -o"GSUtil:sliced_object_download_threshold=1" \
  -o"GSUtil:sliced_object_download_max_components=96" \
  -o"GSUtil:parallel_process_count=32" \
  -o"GSUtil:parallel_thread_count=3" \
  cp gs://testbucket/images* ./
```

**Result times**: 0:31, 0:31, 0:31
**Result goodput**: 6.9Gbps

#### gcsfast

```shell
time gsutil ls gs://testbucket/images* | gcsfast download-many -
```

**Result times**: 0:14, 0:14, 0:13
**Result goodput**: 15.64Gbps

#### Analysis

gcsfast is **125% faster**, with a goodput gain of 8.74Gbps.

### Upload (1 x 2.5GiB file)

#### gsutil

```shell
time gsutil -m \
  -o"GSUtil:parallel_composite_upload_threshold=1" \
  -o"GSUtil:parallel_process_count=32" \
  -o"GSUtil:parallel_thread_count=3" \
  cp ./image1 gs://testbucket/image1
```

***Result times***: 0:06.2, 0:06.0, 0:05.9
***Result goodput***: 3.5Gbps

*Note:* Manually setting the composite component size to the same used for
gcsfast below, and it was slower.

#### gcsfast

```shell
time gcsfast upload -s $((128 * 2 ** 20)) gs://muhtestbucket/image1 image1
```

**Result times**: 0:04.2, 0:04.3, 0:04.2
**Result goodput**: 5.1Gbps

#### Analysis

gcsfast is **45% faster**, with a goodput gain of 1.6Gbps.
