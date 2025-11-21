
with aggregated_data as (
    select 
        passenger_count,
        sum(fare_amount) as total_fare_amount,
        sum(tip_amount) as total_tip_amount
    from {{ source('sling_data', 'taxi_id_test') }}
    group by passenger_count
)

select 
    passenger_count,
    total_fare_amount,
    total_tip_amount
from aggregated_data
where total_fare_amount > 0
order by total_fare_amount desc

