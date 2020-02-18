# gcsfast

Experimental fast downloader for Google Cloud Storage.

## Usage

```
gcsfast download --help
Usage: gcsfast download [OPTIONS] OBJECT_PATH [FILE_PATH]

  Download a GCS object as fast as possible.

  OBJECT_PATH is the path to the object (use gs:// protocol). FILE_PATH is
  the filesystem path for the downloaded object.

Options:
  -p, --processes INTEGER  Set number of processes for simultaneous downloads.
  --help                   Show this message and exit.
```

### Example

`gcsfast -l DEBUG download -p8 gs://mybucket/myblob`