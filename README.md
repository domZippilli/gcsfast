# gcsfast

Experimental fast downloader for Google Cloud Storage.

## Installation

Clone this repo using `git clone ...`

`cd` into the repo and run `pip install .` or `pip install -e .` if you plan on making and running code edits.

## Usage

```
Usage: gcsfast download [OPTIONS] OBJECT_PATH [FILE_PATH]

  Download a GCS object as fast as possible.

  OBJECT_PATH is the path to the object (use gs:// protocol).

  FILE_PATH is the filesystem path for the downloaded object.

Options:
  -p, --processes INTEGER       Set number of processes for simultaneous
                                downloads. Default is
                                multiprocessing.cpu_count().
  -t, --threads INTEGER         Set number of threads (per process) for
                                simultaneous downloads. Default is 1.
  -i, --io_buffer INTEGER       Set io.DEFAULT_BUFFER_SIZE, which determines
                                the size of writes to disk, in bytes. Default
                                is 128KB.
  -n, --min_slice INTEGER       Set the minimum slice size to use, in bytes.
                                Default is 64MiB.
  -m, --max_slice INTEGER       Set the maximum slice size to use, in bytes.
                                Default is 1GiB.
  -s, --slice_size INTEGER      Set the slice size to use, in bytes. Use this
                                to override the slice calculation with your
                                own value.
  -c, --transfer_chunk INTEGER  Set the GCS transfer chunk size to use, in
                                bytes. Must be a multiple of 262144. Default
                                is 262144 * 4 * 16 (16MiB), which covers most
                                cases quite well. Recommend setting this using
                                shell evaluation, e.g. $((262144 * 4 *
                                DESIRED_MB)).
  --help                        Show this message and exit.
```

### Example

`gcsfast -l DEBUG download -p8 gs://mybucket/myblob`