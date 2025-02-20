create table if not exists users (
    id bigserial primary key,
    name text not null,
    age integer
)
