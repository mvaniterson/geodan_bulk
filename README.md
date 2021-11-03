# geodan_bulk
Make bulk requests to the Geodan Routing API to obtain travel distances between multiple pairs of points. 

1. Specify your servicekey in `single_request.py`

1. Provide a pair of points see `get_pairs`-function in `geodan_bulk.py` 

1. Execute `geodan_bulk.py` provide maximale haversine distance between points 

For example to obtain all travel distances between PC4 pairs that are maximally 5km separate run: 

```python
input_file = '../data/pc4.csv'
output_file = '../data/travel_distances.csv'
data = get_pairs(input_file)
geodan_bulk(data, output_file, max_haversine_distance=5, dry_run=True, verbose=False)`
``` 

pc4 4699 
246854 (<=10km)
 80533 (<= 5km)
  8125 (<= 1km)



