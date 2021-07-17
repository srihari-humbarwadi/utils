"""image_downloader.py: Download and save images from a file containing URLS."""

__author__ = "Srihari Humbarwadi"

import concurrent
import concurrent.futures
import os
import threading
import time
from io import BytesIO

import numpy as np
import pandas as pd
import requests
from absl import app, flags, logging
from PIL import Image
from skimage.io import imsave

flags.DEFINE_integer(
    name='max_workers',
    default=1,
    help='Maximum number of concurrent workers attempting to download')

flags.DEFINE_string(
    name='input_text_file',
    default='',
    help='Text file containing one url one each line')

flags.DEFINE_string(
    name='input_csv_file',
    default='',
    help='CSV file containing one url one each line')

flags.DEFINE_string(
    name='column_name',
    default='image_url',
    help='column containing image urls in CSV. Used only if '
    '`input_csv_file` is set')

flags.DEFINE_string(
    name='output_folder',
    default='images',
    help='Path to the output folder. Images will be saved in this folder')

flags.DEFINE_integer(
    name='max_images',
    default=-1,
    help='Number of images to download, used only if set to a non-zero integer'
    '(for each worker)')

flags.DEFINE_boolean(
    name='shuffle_urls',
    default=False,
    help='Shuffle urls before downloading. Used only when `max_images` is set')

flags.DEFINE_integer(
    name='sleep_time',
    default=1,
    help='Number of seconds to wait before attempting to download'
    '(for each worker)')

flags.DEFINE_integer(
    name='min_sleep_time',
    default=0,
    help='Minimum number of seconds to wait before attempting to download'
    '(for each worker), used only if `random_sleep_time=True`')

flags.DEFINE_integer(
    name='max_sleep_time',
    default=5,
    help='Maximum number of seconds to wait before attempting to download'
    '(for each worker), used only if `random_sleep_time=True`')

flags.DEFINE_integer(
    name='max_attempts',
    default=5,
    help='Maximum number of attempts that workers will try before a url is marked as'
    '"failed"')

flags.DEFINE_boolean(
    name='random_sleep_time',
    default=False,
    help='Randomize wait time, if set to true `sleep_time` will be overided with a'
    'random between `min_sleep_time` and `max_sleep_time`')

flags.DEFINE_boolean(
    name='debug',
    default=False,
    help='Log debug information')

FLAGS = flags.FLAGS

_COUNT = 0
_count_lock = threading.Lock()

def _read_urls_from_text_file(path):
    with open(path, 'r') as _fp:
        urls = [line.strip() for line in _fp.readlines()]
    return urls


def _read_urls_from_csv_file(path, column_name):
    urls = pd.read_csv(path)[column_name].values.tolist()
    return urls


def _dump_failed_urls(urls, path='failed_urls.txt'):
    with open(path, 'w') as _fp:
        for url in urls:
            _fp.writelines(url + '\n')


def download_image_from_url(  # pylint: disable=too-many-arguments
        url,
        output_folder,
        sleep_time=2,
        min_sleep_time=0,
        max_sleep_time=5,
        random_sleep_time=True,
        num_attempts=0,
        max_attempts=3,
        total=0):

    """Downloads and saves images from a urls"""
    global _COUNT  # pylint: disable=global-statement
    def _imread(url):
        """Downloads an images and returns it as an numpy array"""
        headers = {
            'user-agent': '''Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36'''  # pylint: disable=line-too-long
        }
        response = requests.get(url, headers=headers)
        image = Image.open(BytesIO(bytes(response.content)))
        return np.array(image, dtype=np.uint8)

    current_thread_id = threading.current_thread().name
    file_name = os.path.basename(url)
    file_save_path = os.path.join(output_folder, file_name)

    if os.path.exists(file_save_path):
        with _count_lock:
            _COUNT += 1
        logging.warning('[Thread: {}] Image with name: {} already downloaded'.
                        format(current_thread_id, file_name))
        return 1

    if num_attempts == max_attempts:
        logging.info(
            '[Thread: {}] Cannot download image: {} after {} attempts.'.
            format(current_thread_id, file_name, num_attempts))
        return -1

    if not os.path.exists(output_folder):
        os.makedirs(output_folder, exist_ok=True)
        logging.info('Created output folder at {}'.format(output_folder))

    if random_sleep_time:
        sleep_time = np.random.randint(min_sleep_time, max_sleep_time)

    if sleep_time:
        logging.debug('[Thread: {}] Sleeping for {} secs on attempt {}'.format(
            current_thread_id, sleep_time, num_attempts))
        time.sleep(sleep_time)

    try:
        num_attempts += 1
        image = _imread(url)
        logging.info(
            '[Thread: {}] [attempt: {}/{}] Successfully downloaded image: '
            '{}...' .format(current_thread_id, num_attempts,
                max_attempts, file_name[:10]))

    except Exception as _:  # pylint: disable=broad-except
        logging.info(
            '[Thread: {}]  [attempt: {}/{}] Failed downloading image: {} '
            .format(current_thread_id, num_attempts, max_attempts, file_name))
        return download_image_from_url(url=url,
                                       output_folder=output_folder,
                                       min_sleep_time=min_sleep_time,
                                       max_sleep_time=max_sleep_time,
                                       random_sleep_time=random_sleep_time,
                                       num_attempts=num_attempts,
                                       max_attempts=max_attempts)
    imsave(file_save_path, image, check_contrast=False)
    with _count_lock:
        _COUNT += 1
        logging.info(
            '[Thread: {}] [Completed: {}/{}] Saved image: {}... to disk '
            .format(current_thread_id, _COUNT, total, file_name[:10]))

    return 1


def main(args):  # pylint: disable=missing-function-docstring, too-many-branches

    del args

    if FLAGS.debug:
        logging.set_verbosity(logging.DEBUG)
        logging.debug('LOGGING DEBUG MESSAGES')
    else:
        logging.set_verbosity(logging.INFO)

    if not FLAGS.input_text_file == '':
        urls = _read_urls_from_text_file(path=FLAGS.input_text_file)

    elif not FLAGS.input_csv_file == '':
        urls = _read_urls_from_csv_file(
            path=FLAGS.input_csv_file,
            column_name=FLAGS.column_name)

    else:
        raise ValueError('No text file or csv file given!!!')

    if FLAGS.max_images > 0:
        if FLAGS.shuffle_urls:
            np.random.shuffle(urls)
        urls = urls[:FLAGS.max_images]

    total = len(urls)
    logging.warning('Downloading {} urls'.format(total))

    status = {}
    future_to_url = {}


    if FLAGS.random_sleep_time:
        timeout = (FLAGS.max_sleep_time * FLAGS.max_attempts)
    else:
        timeout = (FLAGS.sleep_time * FLAGS.max_attempts)
    logging.warning('Setting timeout={} seconds'.format(timeout))

    tik = time.time()
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=FLAGS.max_workers,
            thread_name_prefix='worker') as executor:
        for url in urls:
            future = executor.submit(
                download_image_from_url,
                url=url,
                output_folder=FLAGS.output_folder,
                sleep_time=FLAGS.sleep_time,
                min_sleep_time=FLAGS.min_sleep_time,
                max_sleep_time=FLAGS.max_sleep_time,
                random_sleep_time=FLAGS.random_sleep_time,
                num_attempts=0,
                max_attempts=FLAGS.max_attempts,
                total=total)
            future_to_url[future] = url

        for future in concurrent.futures.as_completed(future_to_url):
            try:
                status[future_to_url[future]] = future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                status[future_to_url[future]] = -1
    tok = time.time()

    failed_urls = [url for url, result in status.items() if result == -1]
    if failed_urls:
        _dump_failed_urls(failed_urls)
        logging.warning(
            'Failed downloading {} urls. Dumping failed urls at `failed_urls.txt`'.
            format(len(failed_urls)))
    else:
        logging.info('Successfully downloading {} urls in {:.2f} secs'.format(
            len(urls), tok - tik))


if __name__ == '__main__':
    app.run(main)
