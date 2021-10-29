import timeit
import os
import pandas as pd
import numpy as np

from csv import writer
from geodanbulk.multi_threaded_requests import multi_threaded_requets, MAX_CONCURRENT_REQUESTS


def get_pairs():
    """Return pd.DataFrame with postcode pairs and their lon/lat
    *_from -> *_to
    """
    pc4 = pd.read_csv('../data/pc4.csv', usecols=['postcode','latitude','longitude'])
    # cross product
    pc4['key'] = 1
    return pc4.merge(pc4, on='key', suffixes=('_from', '_to')).drop('key', axis=1)


def geodan_bulk(data, output_file, dry_run=True, verbose=True):
    """
    Construct full travel distance matrix written directly to a file

    :param data: DataFrame
    :param output_file:
    :param dry_run: default True
    :param verbose: default True
    :return: nothing output written to file
    """

    header = ['from_pc5', 'to_pc5', 'travel_distance', 'travel_time']

    # open file if exists overwrite and add header
    with open(output_file, 'w', newline='') as file_object:
        writer_object = writer(file_object)
        writer_object.writerow(header)

    start = timeit.default_timer()

    # open file append rows
    with open(output_file, 'a', newline='') as file_object:
        writer_object = writer(file_object)

        for chunk in np.array_split(data, MAX_CONCURRENT_REQUESTS):

            from_pc5 = chunk['from_pc5']
            from_x = chunk['from_x']
            from_y = chunk['from_y']
            to_pc5 = chunk['to_pc5']
            to_x = chunk['to_x']
            to_y = chunk['to_y']

            if len(to_x) > 1:
                travel_distances = multi_threaded_requets(from_x, from_y, to_x, to_y, dry_run, verbose)
                for i, travel_distance in enumerate(travel_distances):
                    row = [from_pc5[i], to_pc5[i], travel_distance['distance'], travel_distance['time']]
                    writer_object.writerow(row)

        file_object.close()

    stop = timeit.default_timer()
    execution_time = stop - start
    print("Program Executed in " + str(execution_time) + " seconds.")

    return output_file


if __name__ == '__main__':

    print(os.getcwd())
    data = get_pairs()
    output_file = 'data/travel_distances.csv'

    geodan_bulk(data, output_file, dry_run=True, verbose=True)