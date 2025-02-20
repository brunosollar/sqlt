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

    const conn = try Sqlite.open(":memory:");
    defer conn.close();

    try sqlt.migrate(conn);

    try conn.execute(
        \\insert into users (name, age) values (?, ?)
    , .{ "Alice", 25 });

    try conn.execute(
        \\insert into users (name, age) values (?, ?)
    , .{ "Jane", 99 });

    try conn.execute(
        \\insert into users (name, age) values (?, ?)
    , .{ "Girl", 7 });

    try conn.execute(
        \\insert into users (name, age) values (?, ?)
    , .{ "Adam", null });

    const john = try conn.fetch_optional(allocator, User,
        \\ select name, age from users
        \\ where name = ?
    , .{"John"});

    const all_users = try conn.fetch_all(allocator, User,
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
