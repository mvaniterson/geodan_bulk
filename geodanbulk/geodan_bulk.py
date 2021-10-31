import sys
# use to run using vscode
sys.path.append("C:/Users/mvite/geodan_bulk")

import timeit
import os
import pandas as pd
import numpy as np

from math import radians, cos, sin, asin, sqrt
from csv import writer
from geodanbulk.multi_threaded_requests import multi_threaded_requests, MAX_CONCURRENT_REQUESTS


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
        x (str, optional): longitude column name. Defaults to 'longitude'.
        y (str, optional): latitude column name. Defaults to 'latitude'.
        max_haversine_distance (int, optional): Maximal haversine distance between pairs. Defaults to 10000.

    Returns:
        pd.DataFrame: DataFrame with columns postcode_from, x_from, y_from, postcode_to, x_to, y_to
    """    
    
    data = pd.read_csv(file, usecols=[pc, x, y], 
                       nrows=5) 
    data.columns = ['postcode', 'x', 'y']

    # cross product
    data['key'] = 1
    data = data.merge(data, on='key', suffixes=('_from', '_to')).drop('key', axis=1)

    data['haversine_distance'] = data.apply(
        lambda x: haversine(lon1=x['x_from'],
                            lat1=x['y_from'],
                            lon2=x['x_to'],
                            lat2=x['y_to'])
                            , axis=1)

    return data[data['haversine_distance'] <= max_haversine_distance]


def geodan_bulk(data, output_file, dry_run=True, verbose=True):
    """
    Construct full travel distance matrix written directly to a file

    :param data: DataFrame
    :param output_file:
    :param dry_run: default True
    :param verbose: default True
    :return: nothing output written to file
    """

    header = ['postcode_from', 'postcode_to',
              'haversine_distance', 'travel_distance', 'travel_time']

    # open file if exists overwrite and add header
    with open(output_file, 'w', newline='') as file_object:
        writer_object = writer(file_object)
        writer_object.writerow(header)

    start = timeit.default_timer()

    # open file append rows
    with open(output_file, 'a', newline='') as file_object:
        writer_object = writer(file_object)

        for chunk in np.array_split(data, MAX_CONCURRENT_REQUESTS):
            
            from_pc = chunk['postcode_from']
            from_x = chunk['x_from']
            from_y = chunk['y_from']
            to_pc = chunk['postcode_to']
            to_x = chunk['x_to']
            to_y = chunk['y_to']

            # ASUME: input order is output order?
            distances = multi_threaded_requests(from_x, from_y, to_x, to_y, dry_run, verbose)
            
            chunk['travel_distance'] = [d['distance'] for d in distances]
            chunk['travel_time'] = [d['time'] for d in distances]

            for _, row in chunk[header].iterrows():
                writer_object.writerow(row.values.tolist())
            
        file_object.close()

    stop = timeit.default_timer()
    execution_time = stop - start
    print("Program Executed in " + str(execution_time) + " seconds.")

    return output_file


if __name__ == '__main__':

    print(os.getcwd())
    print(sys.path)

    input_file = 'data/pc4.csv'
    output_file = 'data/travel_distances.csv'

    data = get_pairs(input_file)
    
    print(data.head())
    geodan_bulk(data, output_file, dry_run=True, verbose=True)