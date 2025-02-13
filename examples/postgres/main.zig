const std = @import("std");
const sqlt = @import("sqlt");

const tardy = @import("tardy");
const Tardy = tardy.Tardy(.auto);
const Runtime = tardy.Runtime;
const Socket = tardy.Socket;

const Postgres = sqlt.Postgres;

fn main_frame(rt: *Runtime) !void {
    var postgres = try Postgres.open(rt, "127.0.0.1", 5432, .{ .user = "postgres" });
    defer postgres.close();
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
