const std = @import("std");
const sqlt = @import("sqlt");

const Sqlite = sqlt.Sqlite;

const User = struct {
    name: []const u8,
    age: i32,
    weight: f32 = 10.0,
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

    try connection.execute(
        \\insert into users (name, age) values (?, ?)
    , .{ "Jane", @as(i32, 99) });

    try connection.execute(
        \\insert into users (name, age) values (?, ?)
    , .{ "Girl", @as(i32, 7) });

    const john = try connection.fetch_optional(allocator, User,
        \\ select name, age from users
        \\ where name = ?
    , .{"John"});

    const all_users = try connection.fetch_all(allocator, User,
        \\ select name, age from users
    , .{});

    std.debug.print("john is {?}\n", .{john});

    for (all_users) |user| std.debug.print(
        "{s}'s age: {d} + weight: {?d}\n",
        .{ user.name, user.age, user.weight },
    );
}
