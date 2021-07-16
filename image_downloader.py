import concurrent
import concurrent.futures
from io import BytesIO
import os
import requests
import threading
import time

import numpy as np
from absl import app, flags, logging
from PIL import Image
from skimage.io import imsave

flags.DEFINE_integer(
    name='max_workers',
    default=1,
    help='Maximum number of concurrent workers attempting to download')

flags.DEFINE_string(name='input_urls',
                    default='',
                    help='Text file containing one url one each line')

flags.DEFINE_string(
    name='output_folder',
    default='images',
    help='Path to the output folder. Images will be saved in this folder')

flags.DEFINE_integer(
    name='max_images',
    default=-1,
    help='Number of images to download, used only if set to a non-zero integer'
    '(for each worker)')

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
    help=
    'Maximum number of attempts that workers will try before a url is marked as'
    '"failed"')

flags.DEFINE_boolean(
    name='random_sleep_time',
    default=False,
    help=
    'Randomize wait time, if set to true `sleep_time` will be overided with a'
    'random between `min_sleep_time` and `max_sleep_time`')

flags.DEFINE_boolean(
    name='debug',
    default=False,
    help=
    'Log debug information')

FLAGS = flags.FLAGS


def _read_urls_from_file(path):
    with open(path, 'r') as f:
        urls = [line.strip() for line in f.readlines()]
    return urls


def _dump_failed_urls(urls, path='failed_urls.txt'):
    with open(path, 'w') as f:
        for url in urls:
            f.writelines(url + '\n')


def download_image_from_url(url,
                            output_folder,
                            sleep_time=2,
                            min_sleep_time=0,
                            max_sleep_time=5,
                            random_sleep_time=True,
                            num_attempts=1,
                            max_attempts=3):
    def _imread(url):
      headers = {
        'user-agent': '''Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36'''
        }
      response = requests.get(url, headers=headers)
      image = Image.open(BytesIO(bytes(response.content)))
      return np.array(image, dtype=np.uint8)


    current_thread_id = threading.current_thread().name
    file_name = os.path.basename(url)
    file_save_path = os.path.join(output_folder, file_name)

    if num_attempts > max_attempts:
        logging.info(
            '[Thread: {}] Cannot download image: {} even after {} attempts'.
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
            '[Thread: {}] Successfully downloaded image: {}... on attempt {}/{}'
            .format(current_thread_id, file_name[:10], num_attempts - 1,
                    max_attempts))
  
    except Exception as _:
        logging.info(
            '[Thread: {}] Failed downloading image: {} on attempt {}/{}'.format(
                current_thread_id, file_name, num_attempts - 1, max_attempts))
        return download_image_from_url(url=url,
                                       output_folder=output_folder,
                                       min_sleep_time=min_sleep_time,
                                       max_sleep_time=max_sleep_time,
                                       random_sleep_time=random_sleep_time,
                                       num_attempts=num_attempts,
                                       max_attempts=max_attempts)
    imsave(file_save_path, image, check_contrast=False)
    return 1


def main(args):
    del args
    
    if FLAGS.debug:
        logging.set_verbosity(logging.DEBUG)
        logging.debug('LOGGING DEBUG MESSAGES')
    else:
        logging.set_verbosity(logging.INFO)

    urls = _read_urls_from_file(FLAGS.input_urls)
    if FLAGS.max_images > 0:
        np.random.shuffle(urls)
        urls = urls[:FLAGS.max_images]
        logging.warning('Shuffled urls list and download only {} urls'.format(
            FLAGS.max_images))

    status = {}
    future_to_url = {}
    
    tik = time.time()
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=FLAGS.max_workers,
            thread_name_prefix='worker') as executor:
        for url in urls:
            future = executor.submit(download_image_from_url,
                                     url=url,
                                     output_folder=FLAGS.output_folder,
                                     sleep_time=FLAGS.sleep_time,
                                     min_sleep_time=FLAGS.min_sleep_time,
                                     max_sleep_time=FLAGS.max_sleep_time,
                                     random_sleep_time=FLAGS.random_sleep_time,
                                     num_attempts=1,
                                     max_attempts=FLAGS.max_attempts)
            future_to_url[future] = url

        for future in concurrent.futures.as_completed(future_to_url):
            status[future_to_url[future]] = 'done' if future.result(
            ) == 1 else 'failed'
    tok = time.time()

    failed_urls = [url for url, result in status.items() if result == 'failed']
    if failed_urls:
        _dump_failed_urls(failed_urls)
        logging.warning(
            'Failed downloading {} urls. Dumping failed urls at `failed_urls.txt`'.
            format(len(failed_urls)))
    else:
        logging.info('Successfully downloading {} urls in {:.2f} secs'.format(len(urls), tok - tik))

if __name__ == '__main__':
    app.run(main)
    
