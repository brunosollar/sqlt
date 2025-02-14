const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.@"sqlt/postgres");
const tardy = @import("tardy");

const Message = @import("message.zig").Message;
const Wire = @import("wire.zig").Wire;

const Runtime = tardy.Runtime;
const Socket = tardy.Socket;

pub const Postgres = struct {
    rt: *Runtime,
    allocator: std.mem.Allocator,
    socket: Socket,
    wire: Wire,

    const ConnectOptions = struct {
        user: []const u8,
        password: ?[]const u8 = null,
        database: ?[]const u8 = null,
        replication: enum { true, false, database } = .false,
    };

    pub fn connect(
        allocator: std.mem.Allocator,
        rt: *Runtime,
        host: []const u8,
        port: u16,
        options: ConnectOptions,
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
        wire.send_buffer.clearRetainingCapacity();

        while (true) {
            const recv_buffer = try wire.next_recv();
            const read = try connected.recv(rt, recv_buffer);
            wire.mark_recv(read);

            while (try wire.process_recv()) |m| {
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
        }
    }

    fn bind_param(
        allocator: std.mem.Allocator,
        value: anytype,
        format: *i16,
    ) !Message.Frontend.Bind.Parameter {
        const T = @TypeOf(value);
        switch (@typeInfo(T)) {
            .Int => |info| {
                const I = switch (info.bits) {
                    0...16 => if (info.signedness == .signed) i16 else u16,
                    17...32 => if (info.signedness == .signed) i32 else u32,
                    33...64 => if (info.signedness == .signed) i64 else u64,
                    else => @compileError("Unsupported int bit count"),
                };
                format.* = 1;
                const buf = try allocator.alloc(u8, @sizeOf(I));
                std.mem.writeInt(I, buf[0..@sizeOf(I)], value, .big);
                return .{ .length = @sizeOf(I), .value = buf };
            },
            .ComptimeInt => {
                const I = i64;
                format.* = 1;
                var buf = try allocator.alloc(u8, @sizeOf(I));
                std.mem.writeInt(I, buf[0..@sizeOf(I)], value, .big);
                return .{ .length = @sizeOf(I), .value = buf };
            },
            .Float => |info| {
                const F = switch (info.bits) {
                    0...32 => f32,
                    33...64 => f64,
                    else => @compileError("Unsupported float bit count"),
                };
                format.* = 1;

                const buf = try allocator.alloc(u8, @sizeOf(F));

                // writes the float in network (big) order
                switch (builtin.cpu.arch.endian()) {
                    .big => std.mem.copyForwards(u8, buf, std.mem.asBytes(&value)),
                    .little => {
                        const bytes = std.mem.asBytes(&value);
                        for (0..buf.len) |i| {
                            buf[buf.len - 1 - i] = bytes[i];
                        }
                    },
                }

                return .{ .length = @sizeOf(F), .value = buf };
            },
            .ComptimeFloat => {
                const F = f64;
                format.* = 1;
                const buf = try allocator.alloc(u8, @sizeOf(F));

                // writes the float in network (big) order
                switch (builtin.cpu.arch.endian()) {
                    .big => std.mem.copyForwards(u8, buf, std.mem.asBytes(&value)),
                    .little => {
                        const bytes = std.mem.asBytes(&value);
                        for (0..buf.len) |i| {
                            buf[buf.len - 1 - i] = bytes[i];
                        }
                    },
                }

                return .{ .length = @sizeOf(F), .value = buf };
            },
            .Optional => |_| if (value) |v| {
                return try bind_param(allocator, v, format);
            } else {
                format.* = 1;
                return .{ .length = -1, .value = "" };
            },
            .Null => {
                format.* = 1;
                return .{ .length = -1, .value = "" };
            },
            .Pointer => |ptr_info| switch (ptr_info.size) {
                .Slice => switch (ptr_info.child) {
                    u8 => {
                        format.* = 0;
                        const buf = try allocator.dupe(u8, value);
                        return .{ .length = value.len, .value = buf };
                    },
                    else => @compileError("Unsupported slice type: " ++ @typeName(T)),
                },
                .One => return try bind_param(allocator, value.*, format),
                else => @compileError("Unsupported pointer type: " ++ @typeName(T)),
            },
            .Array => |info| switch (info.child) {
                u8 => {
                    format.* = 0;
                    const buf = try allocator.dupe(u8, &value);
                    return .{ .length = value.len, .value = buf };
                },
                else => @compileError("Unsupported array type: " ++ @typeName(T)),
            },
            .Bool => {
                format.* = 1;
                const buf = try allocator.alloc(u8, 1);
                buf[0] = @intFromBool(value);
                return .{ .length = 1, .value = buf };
            },
            else => @compileError("Unsupported type for postgres binding: " ++ @typeName(T)),
        }
    }

    fn bind_params(
        allocator: std.mem.Allocator,
        formats: []i16,
        parameters: []Message.Frontend.Bind.Parameter,
        params: anytype,
    ) !void {
        const params_info = @typeInfo(@TypeOf(params));
        if (params_info != .Struct) @compileError("params must be a tuple or struct");

        inline for (params_info.Struct.fields, 0..) |field, i| {
            const value = @field(params, field.name);
            parameters[i] = try bind_param(allocator, value, &formats[i]);
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
            var parse_msg = Message.Frontend.Parse{ .query = sql };
            try parse_msg.print(writer);

            const formats = try self.allocator.alloc(i16, field_count);
            defer self.allocator.free(formats);

            const parameters = try self.allocator.alloc(Message.Frontend.Bind.Parameter, field_count);
            defer self.allocator.free(parameters);

            var arena = std.heap.ArenaAllocator.init(self.wire.allocator);
            defer arena.deinit();
            try bind_params(arena.allocator(), formats, parameters, params);

            var bind_msg = Message.Frontend.Bind{ .formats = formats, .parameters = parameters };
            try bind_msg.print(writer);

            var execute_msg = Message.Frontend.Execute{ .row_count = 0 };
            try execute_msg.print(writer);

            var close_msg = Message.Frontend.Close{ .kind = .portal };
            try close_msg.print(writer);

            try Message.Frontend.Sync.print(writer);
            _ = try self.socket.send_all(self.rt, self.wire.send_buffer.items);
            self.wire.send_buffer.clearRetainingCapacity();

            while (true) {
                const recv_buffer = try self.wire.next_recv();
                const read = try self.socket.recv(self.rt, recv_buffer);
                self.wire.mark_recv(read);
                while (try self.wire.process_recv()) |m| switch (m) {
                    .ReadyForQuery => return,
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
