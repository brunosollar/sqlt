const std = @import("std");
const log = std.log.scoped(.@"sqlt/postgres");
const tardy = @import("tardy");

const Message = @import("message.zig").Message;
const StartupMessage = @import("message.zig").StartupMessage;
const Wire = @import("wire.zig").Wire;

const Runtime = tardy.Runtime;
const Socket = tardy.Socket;

pub const Postgres = struct {
    socket: Socket,
    wire: Wire,

    const OpenOptions = struct {
        user: []const u8,
        password: ?[]const u8 = null,
        database: ?[]const u8 = null,
        replication: enum { true, false, database } = .false,
    };

    pub fn open(
        rt: *Runtime,
        host: []const u8,
        port: u16,
        options: OpenOptions,
    ) !Postgres {
        const addresses = try std.net.getAddressList(rt.allocator, host, port);
        defer addresses.deinit();

        const connected: Socket = blk: {
            for (addresses.addrs) |addr| {
                const socket = try Socket.init_with_address(.tcp, addr);
                break :blk socket.connect(rt) catch continue;
            }

            return error.ConnectionFailed;
        };

        var wire = try Wire.init(rt.allocator);
        errdefer wire.deinit();

        var builder = try StartupMessage.Builder.init(rt.allocator);
        try builder.add_parameter(.{ "user", options.user });
        if (options.password) |password| try builder.add_parameter(.{ "password", password });
        if (options.database) |database| try builder.add_parameter(.{ "database", database });
        try builder.add_parameter(.{ "replication", @tagName(options.replication) });

        const msg = try builder.build();
        defer rt.allocator.free(msg.payload);

        try msg.write(wire.send_buffer.writer(wire.allocator));
        _ = try connected.send_all(rt, wire.send_buffer.items);
        wire.send_buffer.clearRetainingCapacity();

        while (true) {
            const recv_buffer = try wire.next_recv();
            const read = try connected.recv(rt, recv_buffer);
            wire.mark_recv(read);

            log.debug("read count={d}", .{read});
            while (try wire.process_recv()) |m| {
                log.debug("msg type: {s}", .{@tagName(m)});
                switch (m) {
                    .ReadyForQuery => return .{ .socket = connected, .wire = wire },
                    .ErrorResponse => |_| return error.ConnectionFailed,
                    .ParameterStatus => |inner| {
                        log.info("parameter: {s}={s}", .{ inner.name, inner.value });
                    },
                    else => continue,
                }
            }

            log.info("processed={d}", .{wire.bytes_processed});
        }

        return .{ .socket = connected };
    }

    pub fn close(self: *Postgres) void {
        self.socket.close_blocking();
        self.wire.deinit();
    }
};
