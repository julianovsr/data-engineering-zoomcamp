select *

from {{source('raw_data','yellow_trip_data_partitioned_clustered')}}