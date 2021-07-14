# utils

___

```
  usage: python image_downloader.py <flags>
  list of flags
  --[no]debug: Log debug information
    (default: 'false')
  --input_urls: Text file containing one url one each line
    (default: '')
  --max_attempts: Maximum number of attempts that workers will try before a url is marked as"failed"
    (default: '5')
    (an integer)
  --max_images: Number of images to download, used only if set to a non-zero integer(for each worker)
    (default: '-1')
    (an integer)
  --max_sleep_time: Maximum number of seconds to wait before attempting to download(for each worker), used only if `random_sleep_time=True`
    (default: '5')
    (an integer)
  --max_workers: Maximum number of concurrent workers attempting to download
    (default: '1')
    (an integer)
  --min_sleep_time: Minimum number of seconds to wait before attempting to download(for each worker), used only if `random_sleep_time=True`
    (default: '0')
    (an integer)
  --output_folder: Path to the output folder. Images will be saved in this folder
    (default: 'images')
  --[no]random_sleep_time: Randomize wait time, if set to true `sleep_time` will be overided with arandom between `min_sleep_time` and `max_sleep_time`
    (default: 'false')
  --sleep_time: Number of seconds to wait before attempting to download(for each worker)
    (default: '1')
    (an integer)
```
___
