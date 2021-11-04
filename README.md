# geodan_bulk
Make bulk requests to the Geodan Routing API to obtain travel distances between multiple pairs of points. 

1. Specify your servicekey in `single_request.py`

1. Provide a pair of points see `get_pairs`-function in `geodan_bulk.py` 

1. Execute `geodan_bulk.py` provide maximale haversine distance between points 

For example to obtain all travel distances between PC4 pairs that are maximally 5km separate run: 

```python
from geodanbulk import get_data, geodan_bulk

input_file = '../data/pc4.csv'
output_file = '../data/travel_distances.csv'
data = get_data(input_file)
geodan_bulk(data, output_file, max_haversine_distance=5, dry_run=True, verbose=False)

iteration: (100/33317), 15 requests/second
iteration: (200/33317), 14 requests/second
iteration: (300/33317), 13 requests/second
iteration: (400/33317), 14 requests/second
iteration: (500/33317), 15 requests/second
iteration: (600/33317), 15 requests/second
``` 

Current implementation with maximal 10 concurrent requests can handle around 14 requests/second!


*Table 1*: Obtain travel distances for pc4 (4699 distinct) pairs that are maximally separated with a certain distance:

| Number of pairs | distance (km) | Estimated execution time |
|--------|--------|--------|
| 246854 | 10 | 5h |
|  80533 |  5 | 1.5h |


https://pgrouting.org/


*Table 2*: Obtain travel distances for pc5 (33317 distinct) pairs that are maximally separated with a certain distance:

| Number of pairs | distance (km) | Estimated execution time |
|--------|--------|--------|
| 33317x50 | 2 | >24h |




