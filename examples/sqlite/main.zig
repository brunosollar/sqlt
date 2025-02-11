const std = @import("std");
const sqlt = @import("sqlt");

const Sqlite = sqlt.Sqlite;

const User = struct {
    name: []const u8,
    age: i32,
};

pub fn main() !void {
    const allocator = std.heap.page_allocator;

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

    const alice = try connection.fetch_one(allocator, User,
        \\ select name, age from users
        \\ where name = ?
    , .{"Alice"});

    const john = try connection.fetch_optional(allocator, User,
        \\ select name, age from users
        \\ where name = ?
    , .{"John"});

    std.debug.print("alice's age: {d}\n", .{alice.age});
    std.debug.print("john is {?}\n", .{john});
}
