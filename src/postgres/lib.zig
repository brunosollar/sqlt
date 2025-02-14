const std = @import("std");
const log = std.log.scoped(.@"sqlt/postgres");
const tardy = @import("tardy");

const Message = @import("message.zig").Message;
const Wire = @import("wire.zig").Wire;

const Runtime = tardy.Runtime;
const Socket = tardy.Socket;

pub fn is_zig_string(comptime T: type) bool {
    return comptime blk: {
        const info = @typeInfo(T);
        if (info != .Pointer) break :blk false;
        const ptr = &info.Pointer;
        if (ptr.is_volatile or ptr.is_allowzero) break :blk false;
        if (ptr.size == .Slice) {
            break :blk ptr.child == u8;
        }
        if (ptr.size == .One) {
            const child = @typeInfo(ptr.child);
            if (child == .Array) {
                const arr = &child.Array;
                break :blk arr.child == u8;
            }
        }
        break :blk false;
    };
}

pub const Postgres = struct {
    rt: *Runtime,
    allocator: std.mem.Allocator,
    socket: Socket,
    wire: Wire,

    const OpenOptions = struct {
        user: []const u8,
        password: ?[]const u8 = null,
        database: ?[]const u8 = null,
        replication: enum { true, false, database } = .false,
    };

    pub fn open(
        allocator: std.mem.Allocator,
        rt: *Runtime,
        host: []const u8,
        port: u16,
        options: OpenOptions,
    ) !Postgres {
        const addresses = try std.net.getAddressList(allocator, host, port);
        defer addresses.deinit();

        const connected: Socket = blk: {
            for (addresses.addrs) |addr| {
                const socket = try Socket.init_with_address(.tcp, addr);
                break :blk socket.connect(rt) catch continue;
            }

            return error.ConnectionFailed;
        };

        var wire = try Wire.init(allocator);
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
                    .BackendKeyData => continue,
                    .ParameterStatus => |inner| log.info("parameter: {s}={s}", .{ inner.name, inner.value }),
                    .ReadyForQuery => return .{
                        .rt = rt,
                        .allocator = allocator,
                        .socket = connected,
                        .wire = wire,
                    },
                    .ErrorResponse => |_| return error.ConnectionFailed,
                    .NoticeResponse => continue,
                    else => log.err("unexpected message: {s}", .{@tagName(m)}),
                }
            }

            log.info("processed={d}", .{wire.bytes_processed});
        }
    }

    pub fn execute(self: *Postgres, comptime sql: []const u8, params: anytype) !void {
        const params_info = @typeInfo(@TypeOf(params));
        if (params_info != .Struct) @compileError("params must be a struct");
        const struct_info = params_info.Struct;
        const field_count = struct_info.fields.len;

        const writer = self.wire.send_buffer.writer(self.wire.allocator);

        if (field_count == 0) {
            // If we have no params, just send a simple query.
            var query = Message.Frontend.Query{ .query = sql };
            try query.print(writer);
            _ = try self.socket.send_all(self.rt, self.wire.send_buffer.items);
            self.wire.send_buffer.clearRetainingCapacity();

            while (true) {
                const recv_buffer = try self.wire.next_recv();
                const read = try self.socket.recv(self.rt, recv_buffer);
                self.wire.mark_recv(read);
                while (try self.wire.process_recv()) |m| switch (m) {
                    .ReadyForQuery => return,
                    .ErrorResponse => return error.ConnectionFailed,
                    .NoticeResponse => continue,
                    .CommandComplete => |tag| log.info("command complete: {s}", .{tag}),
                    .ParameterStatus => |inner| log.info(
                        "parameter: {s}={s}",
                        .{ inner.name, inner.value },
                    ),
                    // If we returned rows, just free them.
                    .RowDescription => |columns| self.wire.allocator.free(columns),
                    .DataRow => |columns| self.wire.allocator.free(columns),
                    .EmptyQueryResponse => continue,
                    .CopyInResponse, .CopyOutResponse => continue,
                    else => log.err("unexpected message: {s}", .{@tagName(m)}),
                };
            }
        } else {
            // Parse
            var parse_query = Message.Frontend.Parse{ .query = sql };
            try parse_query.print(writer);
            _ = try self.socket.send_all(self.rt, self.wire.send_buffer.items);
            self.wire.send_buffer.clearRetainingCapacity();

            // Bind
            var formats = try self.allocator.alloc(i16, field_count);
            defer self.allocator.free(formats);

            var parameters = try self.allocator.alloc(Message.Frontend.Bind.Parameter, field_count);
            defer self.allocator.free(parameters);

            var values = std.ArrayList(u8).init(self.wire.allocator);
            defer values.deinit();
            var values_index: usize = 0;

            inline for (struct_info.fields, 0..) |field, i| {
                if (comptime is_zig_string(field.type)) {
                    formats[i] = 0;
                    const field_value = @field(params, field.name);
                    parameters[i] = .{
                        .length = field_value.len,
                        .value = field_value,
                    };
                } else {
                    formats[i] = 1;
                    const field_value = @field(params, field.name);
                    try values.writer().writeInt(i32, field_value, .big);
                    parameters[i] = .{
                        .length = @sizeOf(i32),
                        .value = values.items[values_index..][0..@sizeOf(i32)],
                    };
                    values_index += @sizeOf(i32);
                }
            }

            var bind = Message.Frontend.Bind{ .formats = formats, .parameters = parameters };
            try bind.print(writer);
            _ = try self.socket.send_all(self.rt, self.wire.send_buffer.items);
            self.wire.send_buffer.clearRetainingCapacity();

            // Execute
            var execute_msg = Message.Frontend.Execute{ .row_count = -1 };
            try execute_msg.print(writer);
            _ = try self.socket.send_all(self.rt, self.wire.send_buffer.items);
            self.wire.send_buffer.clearRetainingCapacity();

            var close_msg = Message.Frontend.Close{ .kind = .portal };
            try close_msg.print(writer);
            _ = try self.socket.send_all(self.rt, self.wire.send_buffer.items);
            self.wire.send_buffer.clearRetainingCapacity();

            try Message.Frontend.Sync.print(writer);
            _ = try self.socket.send_all(self.rt, self.wire.send_buffer.items);
            self.wire.send_buffer.clearRetainingCapacity();

            drive: while (true) {
                const recv_buffer = try self.wire.next_recv();
                const read = try self.socket.recv(self.rt, recv_buffer);
                self.wire.mark_recv(read);
                while (try self.wire.process_recv()) |m| switch (m) {
                    .ReadyForQuery => break :drive,
                    .ErrorResponse => return error.Failed,
                    else => log.err("unexpected message: {s}", .{@tagName(m)}),
                };
            }
        }
    }

    pub fn close(self: *Postgres) void {
        self.socket.close_blocking();
        self.wire.deinit();
    }
};
