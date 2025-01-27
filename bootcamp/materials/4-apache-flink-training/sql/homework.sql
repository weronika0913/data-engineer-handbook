--create processed events ip aggregated table
create table processed_events_ip_aggregated (
    event_window_timestamp timestamp(3).
    host varchar,
    ip varchar,
    num_hits bigint
)

--What is the average number of web events of a session from a user on Tech Creator

select ip, host, event_window_timestamp, cast(avg(num_hits) as integer) as average_num_hits
from processed_events_ip_aggregated
where host like '%techcreator.io'
group by ip, host, event_window_timestamp

--Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io

select ip, host, event_window_timestamp, cast (avg(num_hits) as integer) as average_num_hits
from processed_events_ip_aggregated
group by ip, host, event_window_timestamp
