create table if not exists users (
    id integer primary key,
    perms integer not null,
    country integer not null,
    name text not null,
    age integer
);

