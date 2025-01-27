Create type films  as(
        film text, 
        year integer,
        votes integer,
        rating real,
        filmid text
);

create type quality_class as enum(
    'star', 'good', 'average', 'bad'
);

CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
    films films[],
    quality_class quality_class,
    current_year INTEGER,
    is_active BOOLEAN,
    PRIMARY KEY (actorid, current_year)
);