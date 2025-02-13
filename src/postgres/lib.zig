const std = @import("std");
const log = std.log.scoped(.@"sqlt/postgres");
const tardy = @import("tardy");

const Message = @import("message.zig").Message;
const Wire = @import("wire.zig").Wire;

const Runtime = tardy.Runtime;
const Socket = tardy.Socket;

pub const Postgres = struct {
    rt: *Runtime,
    socket: Socket,
    wire: Wire,

    const OpenOptions = struct {
        user: []const u8,
        password: ?[]const u8 = null,
        database: ?[]const u8 = null,
        replication: enum { true, false, database } = .false,
    };

    // Basically, drives the Wire engine until we get a ReadyForQuery
    // or an Error.
    fn drive(self: *Postgres) !void {
        while (true) {
            const recv_buffer = try self.wire.next_recv();
            const read = try self.socket.recv(self.rt, recv_buffer);
            self.wire.mark_recv(read);

            log.debug("read count={d}", .{read});
            while (try self.wire.process_recv()) |m| {
                log.debug("msg type: {s}", .{@tagName(m)});
                switch (m) {
                    .ReadyForQuery => return,
                    .ErrorResponse => |_| return error.ConnectionFailed,
                    .CommandComplete => |tag| {
                        log.info("command complete: {s}", .{tag});
                    },
                    .ParameterStatus => |inner| {
                        log.info("parameter: {s}={s}", .{ inner.name, inner.value });
                    },
                    else => continue,
                }
            }

            log.info("processed={d}", .{self.wire.bytes_processed});
        }
    }

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

        var startup = Message.Frontend.StartupMessage{
            .pairs = &.{
                .{ "user", options.user },
                .{ "database", options.database orelse options.user },
                .{ "replication", @tagName(options.replication) },
            },
        };
        try startup.print(wire.send_buffer.writer(wire.allocator));

        _ = try connected.send_all(rt, wire.send_buffer.items);
        log.debug("not-working startup: {any}", .{wire.send_buffer.items});
        wire.send_buffer.clearRetainingCapacity();

        while (true) {
            const recv_buffer = try wire.next_recv();
            const read = try connected.recv(rt, recv_buffer);
            wire.mark_recv(read);

            log.debug("read count={d}", .{read});
            while (try wire.process_recv()) |m| {
                log.debug("msg type: {s}", .{@tagName(m)});
                switch (m) {
                    .ReadyForQuery => return .{ .rt = rt, .socket = connected, .wire = wire },
                    .ErrorResponse => |_| return error.ConnectionFailed,
                    .ParameterStatus => |inner| {
                        log.info("parameter: {s}={s}", .{ inner.name, inner.value });
                    },
                    else => continue,
                }
            }

            log.info("processed={d}", .{wire.bytes_processed});
        }
    }

    pub fn execute(self: *Postgres, comptime sql: []const u8, params: anytype) !void {
        _ = params;
        var query = Message.Frontend.Query{ .query = sql };
        try query.print(self.wire.send_buffer.writer(self.wire.allocator));
        defer self.wire.send_buffer.clearRetainingCapacity();

        _ = try self.socket.send_all(self.rt, self.wire.send_buffer.items);
        try self.drive();
    }

    pub fn close(self: *Postgres) void {
        self.socket.close_blocking();
        self.wire.deinit();
    }
};
