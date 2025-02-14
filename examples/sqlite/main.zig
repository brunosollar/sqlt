const std = @import("std");
const sqlt = @import("sqlt");

const Sqlite = sqlt.Sqlite;

const User = struct {
    name: []const u8,
    age: ?i32,
    weight: f32 = 10.0,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

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
    , .{ "Alice", 25 });

    try connection.execute(
        \\insert into users (name, age) values (?, ?)
    , .{ "Jane", 99 });

    try connection.execute(
        \\insert into users (name, age) values (?, ?)
    , .{ "Girl", 7 });

    try connection.execute(
        \\insert into users (name, age) values (?, ?)
    , .{ "Adam", null });

    const john = try connection.fetch_optional(allocator, User,
        \\ select name, age from users
        \\ where name = ?
    , .{"John"});

    const all_users = try connection.fetch_all(allocator, User,
        \\ select name, age from users
    , .{});

    defer allocator.free(all_users);
    defer for (all_users) |user| allocator.free(user.name);

    std.debug.print("john is {?}\n", .{john});

    for (all_users) |user| std.debug.print(
        "{s}'s age: {?d} + weight: {?d}\n",
        .{ user.name, user.age, user.weight },
    );
}
