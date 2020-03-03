# gcsfast

Experimental fast downloader for Google Cloud Storage.

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
  download2      Download a stream of GCS objects as fast as possible.
  stream-upload  Stream data of an arbitrary length into an object in GCS.
```

See `--help` on each command for more info.

### Examples

*Download an object to local file*
`gcsfast -l DEBUG download -p8 gs://mybucket/myblob`

*Download a series of files described in a file*
`gcsfast -l DEBUG download2 files.txt`

*Upload from stdin with a fixed slice size*
`gcsfast -l DEBUG stream_upload gs://mybucket/mystream`

*Upload from file/FIFO with a fixed slice size*
`gcsfast -l DEBUG stream_upload gs://mybucket/mystream myfile`
