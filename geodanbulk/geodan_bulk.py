import timeit
import pandas as pd
import numpy as np

from math import radians, cos, sin, asin, sqrt
from csv import writer
from geodanbulk.multi_threaded_requests import multi_threaded_requests, MAX_CONCURRENT_REQUESTS

PROGRESS_EVERY_N = 100

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance in kilometers between two points 
    on the earth (specified in decimal degrees)
    https://en.wikipedia.org/wiki/Haversine_formula
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r


def get_pairs(file, pc='postcode', x= 'longitude', y='latitude', max_haversine_distance=10000):
    """Generates postcode pairs from postcode table:

    1. Read postcode table with longitude and latitude
    2. Generates cross product with all pairs
    3. Make subselect based on maximal haversine distance between pairs

    NOTE: Geodan API use x/y as lon/lat
    Args:
        file (str): file containing postcode, longitude and latitude
        pc (str, optional): Postcode column name. Defaults to 'postcode'.
        x (str, optional): longitude column name. Defaults to 'longitude' i.e. 4.9 .
        y (str, optional): latitude column name. Defaults to 'latitude', i.e. 52.3 .
        max_haversine_distance (int, optional): Maximal haversine distance between pairs. Defaults to 10000.

    Returns:
        pd.DataFrame: DataFrame with columns postcode_from, x_from, y_from, postcode_to, x_to, y_to
    """    

    dtypes = {pc: str, x: np.float64, y: np.float64}
    data = pd.read_csv(file, usecols=[pc, x, y], dtype=dtypes)

    # reorder if necessary
    data = data[[pc, x, y]]
    data.columns = ['postcode', 'x', 'y']
    return data


def geodan_bulk(data, output_file, max_haversine_distance=10, dry_run=True, verbose=True):
    """
    Construct full travel distance matrix written directly to a file

    :param data: DataFrame
    :param output_file:
    :param max_haversine_distance: default 10km
    :param dry_run: default True
    :param verbose: default True
    :return: nothing output written to file
    """

    header = ['postcode_from', 'postcode_to',
              'travel_distance', 'travel_time']

    # open file if exists overwrite and add header
    with open(output_file, 'w', newline='') as file_object:
        writer_object = writer(file_object)
        writer_object.writerow(header)

    start_total = timeit.default_timer()
    start = timeit.default_timer()
    nrequests = 0
    niterations = data.shape[0]
    # open file append rows
    with open(output_file, 'a', newline='') as file_object:
        writer_object = writer(file_object)

        for i, from_pc in data.iterrows():

            if (i % PROGRESS_EVERY_N) == 0 and i > 0:
                stop = timeit.default_timer()
                print(f'iteration: ({i}/{niterations}), {int(nrequests/(stop - start))} requests/second')
                nrequests = 0

            from_x = from_pc['x']
            from_y = from_pc['y']

            haversine_distances = data.apply(lambda x: haversine(lon1=from_x, lat1=from_y,
                                                                 lon2=x['x'], lat2=x['y']),
                                             axis=1)

            within_distance = haversine_distances <= max_haversine_distance
            nrequests += sum(within_distance)
            for chunk in np.array_split(data[within_distance], 1 + sum(within_distance)//MAX_CONCURRENT_REQUESTS):
                to_x = chunk['x'].values
                to_y = chunk['y'].values

                distances = multi_threaded_requests(from_x, from_y, to_x, to_y, dry_run, verbose)

                chunk['postcode_from'] = from_pc['postcode']
                chunk['postcode_to'] = chunk['postcode']
                chunk['travel_distance'] = [d['distance'] for d in distances]
                chunk['travel_time'] = [d['time'] for d in distances]

                for _, row in chunk[header].iterrows():
                    writer_object.writerow(row.values.tolist())
            
        file_object.close()

    stop_total = timeit.default_timer()
    execution_time = stop_total - start_total
    print("Program Executed in " + str(execution_time) + " seconds.")

    return output_file


if __name__ == '__main__':

    input_file = '../data/pc4.csv'
    output_file = '../data/travel_distances.csv'

    data = get_pairs(input_file)

    geodan_bulk(data, output_file, max_haversine_distance=5, dry_run=True, verbose=False)