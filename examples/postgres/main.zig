const std = @import("std");
const sqlt = @import("sqlt");

const tardy = @import("tardy");
const Tardy = tardy.Tardy(.auto);
const Runtime = tardy.Runtime;
const Socket = tardy.Socket;

const Postgres = sqlt.Postgres;

const User = struct {
    name: []const u8,
    age: ?i32,
    weight: f32 = 10.0,
};

fn main_frame(rt: *Runtime) !void {
    var conn = try Postgres.connect(rt.allocator, rt, "127.0.0.1", 5432, .{
        .user = "postgres",
        .database = "postgres",
    });
    defer conn.close();

    try conn.execute("set log_min_messages to 'DEBUG5'", .{});
    try conn.execute("set client_min_messages to 'DEBUG5'", .{});
    try sqlt.migrate(&conn);

    try conn.execute(
        \\insert into users (name, age) values ($1, $2)
    , .{ "Alice", 25 });

    try conn.execute(
        \\insert into users (name, age) values ($1, $2)
    , .{ "Jane", 99 });

    try conn.execute(
        \\insert into users (name, age) values ($1, $2)
    , .{ "Girl", 7 });

    try conn.execute(
        \\insert into users (name, age) values ($1, $2)
    , .{ "Adam", null });

    const john = try conn.fetch_optional(rt.allocator, User,
        \\ select name, age from users
        \\ where name = $1
    , .{"John"});

    const all_users = try conn.fetch_all(rt.allocator, User,
        \\ select name, age from users
    , .{});

    defer rt.allocator.free(all_users);
    defer for (all_users) |user| rt.allocator.free(user.name);

    std.debug.print("john is {?}\n", .{john});

    for (all_users) |user| std.debug.print(
        "{s}'s age: {?d} + weight: {d}\n",
        .{ user.name, user.age, user.weight },
    );
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var t = try Tardy.init(allocator, .{ .threading = .single });
    defer t.deinit();

    try t.entry({}, struct {
        fn entry(rt: *Runtime, _: void) !void {
            try rt.spawn(.{rt}, main_frame, 1024 * 1024 * 4);
        }
    }.entry);
}
