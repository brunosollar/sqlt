create table if not exists users (
    id bigserial primary key,
    perms int4 not null,
    country int4 not null,
    name text not null,
    age integer
)
