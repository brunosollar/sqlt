const std = @import("std");
const sqlt = @import("sqlt");

const Sqlite = sqlt.Sqlite;

pub fn main() !void {
    const connection = try Sqlite.open("./test.db");
    defer connection.close();

    try connection.execute(
        \\create table if not exists users (
        \\id integer primary key,
        \\name text not null,
        \\age integer
        \\)
    , .{});

    try connection.execute(
        \\insert into users (name, age) values (?, ?)
    , .{ "Alice", @as(i32, 25) });
}
