import logging

from concurrent.futures import ThreadPoolExecutor
from geodanbulk.single_request import single_request


MAX_CONCURRENT_REQUESTS = 10
TIMEOUT = 10


# -- Configure logger
logging.basicConfig(
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def multi_threaded_requests(from_x, from_y, to_x, to_y, dry_run=False, verbose=True):
    """
    Get multiple travel distances from Geodan API
    by calling single_request.single_request using multiple threads

    :param from_x: from longitude single float
    :param from_y: from latitude single float
    :param to_x: to longitude multiple floats
    :param to_y: to latitude multiple floats
    :param dry_run: default True
    :param verbose: default True
    :return: travel distances
    """

    nworkers = min(MAX_CONCURRENT_REQUESTS, len(to_x))

    futures_list = []
    results = []
    with ThreadPoolExecutor(max_workers=nworkers) as executor:
        for x, y in zip(to_x, to_y):
            futures = executor.submit(single_request, from_x, from_y, x, y, dry_run, verbose)
            futures_list.append(futures)
        for future in futures_list:
            try:
                result = future.result(timeout=TIMEOUT)
                results.append(result)
            except Exception:
                results.append(None)
    return results


if __name__ == '__main__':

    ncalls = 25
    from_y = ncalls*[52.3729762]
    from_x = ncalls*[4.9039565]
    to_y = ncalls*[52.3733862]
    to_x = ncalls*[4.8940639]

    results = multi_threaded_requets(from_x, from_y, to_x, to_y)

    print(results)
