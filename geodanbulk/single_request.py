import requests
import logging
import json


# -- Configure logger
logging.basicConfig(
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

HOST_URL = 'services.geodan.nl/routing/v2/route/fiets_18kmu'
SERVICEKEY = ''


def single_request(from_x, from_y, to_x, to_y, dry_run=True, verbose=True):
    """
    Get travel distance from Geodan API

    :param from_x: from longitude
    :param from_y: from latitude
    :param to_x: to longitude
    :param to_y: to latitude
    :param dry_run: default True
    :param verbose: default True
    :return: travel distance
    """

    params = {'fromX': from_x,
              'fromY': from_y,
              'toX': to_x,
              'toY': to_y,
              #'networktype': 'fiets_18kmu',  # auto, gsps_nl, vrachtwagen, fiets_18kmu, fiets_25kmu
              'calculationMode': 'time',     # time, distance, cost
              'calcsize': 10,                # integer of time, distance or cost
              'outputformat': 'geojson',     # xml, geojson, json
              'returnType': 'coords',       # coords, timedistance
              'servicekey': SERVICEKEY}

    url = "http://{}?".format(HOST_URL)

    response = []

    if dry_run:
        if verbose:
            print('dry run')
        response = {'distance': -999, 'time': -999}
    else:
        r = requests.get(url, params)
        if verbose:
            print(f'request url: {r.url}')
        if r.status_code == 200:

            if verbose:
                print(f'response time: {r.elapsed.total_seconds()}')
                print('full response')
                print(json.dumps(r.json(), indent=4, sort_keys=True))
            response = {'distance': r.json()['features'][0]['properties']['distance'],
                        'time': r.json()['features'][0]['properties']['time']
                        }
        else:
            print(r.status_code, r.content)

    return response


if __name__ == '__main__':

    from_y, from_x = (52.3729762, 4.9039565)
    to_y, to_x = (52.3733862, 4.8940639)
    resp = single_request(from_x, from_y, to_x, to_y, dry_run=False, verbose=True)

    print(resp)
