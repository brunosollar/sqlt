const std = @import("std");
const tardy = @import("tardy");

const Runtime = tardy.Runtime;
const Socket = tardy.Socket;

pub const Postgres = struct {
    socket: Socket,

    const OpenOptions = struct {
        user: []const u8,
        password: ?[]const u8,
        database: ?[]const u8,
        replication: enum { true, false, database } = .false,
    };

    pub fn open(
        allocator: std.mem.Allocator,
        rt: *Runtime,
        host: []const u8,
        port: u16,
        options: OpenOptions,
    ) !Postgres {
        _ = options;
        const addresses = try std.net.getAddressList(allocator, host, port);
        defer addresses.deinit();

        const connected: Socket = blk: {
            for (addresses.addrs) |addr| {
                const socket = try Socket.init_with_address(.tcp, addr);
                break :blk try socket.connect(rt) catch continue;
            }

            return error.ConnectionFailed;
        };

        _ = connected;
    }
};
