const std = @import("std");
const sqlt = @import("sqlt");

const Sqlite = sqlt.Sqlite;

const User = struct {
    const Permission = enum(u32) { none = 0, admin = 1 };
    const Country = enum { usa, canada, united_kingdom };

    name: []const u8,
    perms: Permission,
    country: Country,
    age: ?i32,
    weight: f32 = 10.0,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const conn = try Sqlite.open(":memory:");
    defer conn.close();

    try sqlt.migrate(allocator, conn);

    try conn.execute(
        \\insert into users (name, age, perms, country) values (?, ?, ?, ?)
    , .{ "Alice", 25, User.Permission.admin, User.Country.usa });

    try conn.execute(
        \\insert into users (name, age, perms, country) values (?, ?, ?, ?)
    , .{ "Jane", 99, User.Permission.none, User.Country.canada });

    try conn.execute(
        \\insert into users (name, age, perms, country) values (?, ?, ?, ?)
    , .{ "Girl", 7, User.Permission.none, User.Country.usa });

    try conn.execute(
        \\insert into users (name, age, perms, country) values (?, ?, ?, ?)
    , .{ "Adam", null, User.Permission.none, User.Country.usa });

    const john = try conn.fetch_optional(allocator, User,
        \\ select name, age, perms, country from users
        \\ where name = ?
    , .{"John"});

    const all_users = try conn.fetch_all(allocator, User, "select * from users", .{});

    defer allocator.free(all_users);
    defer for (all_users) |user| allocator.free(user.name);

    std.debug.print("john is {?}\n", .{john});

    for (all_users) |user| std.debug.print(
        "{s}'s age: {?d} + weight: {?d} | perms: {s} + country: {s}\n",
        .{ user.name, user.age, user.weight, @tagName(user.perms), @tagName(user.country) },
    );
}
