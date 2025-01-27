-- DDL for host cumulated table
create table host_cumulated (
    host,
    host_activity_datelist DATE[],
    date,
    primary key(host,date)
)