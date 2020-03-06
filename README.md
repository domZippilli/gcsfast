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

`gcsfast -l DEBUG download -p8 gs://mybucket/myblob`

*Download a series of objects described in a file*

`gcsfast -l DEBUG download-many files.txt`

*Upload from stdin with a fixed slice size*

`gcsfast -l DEBUG upload-stream gs://mybucket/mystream`

*Upload from file/FIFO with a fixed slice size*

`gcsfast -l DEBUG upload-stream gs://mybucket/mystream myfile`
