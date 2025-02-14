const std = @import("std");
const sqlt = @import("sqlt");

const tardy = @import("tardy");
const Tardy = tardy.Tardy(.auto);
const Runtime = tardy.Runtime;
const Socket = tardy.Socket;

const Postgres = sqlt.Postgres;

fn main_frame(rt: *Runtime) !void {
    var connection = try Postgres.connect(rt.allocator, rt, "127.0.0.1", 5432, .{
        .user = "postgres",
        .database = "postgres",
    });
    defer connection.close();

    try connection.execute("set log_min_messages to 'DEBUG5'", .{});
    try connection.execute("set client_min_messages to 'DEBUG5'", .{});

    try connection.execute(
        \\create table if not exists users (
        \\id bigserial primary key,
        \\name text not null,
        \\age integer,
        \\weight real not null
        \\)
    , .{});

    try connection.execute(
        \\insert into users (name, age, weight)
        \\values ($1, $2, $3)
    , .{ "John", null, @as(f32, 10.2) });
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
