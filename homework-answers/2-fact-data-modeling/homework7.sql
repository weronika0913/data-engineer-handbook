-- DDL for host activity reduced
create table host_activity_reduced(
    host text,
    month_start date,
    hit_array REAL[],
    unique_visitor REAL[],
    primary key (host, month_start)
)
