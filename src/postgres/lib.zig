const std = @import("std");
const assert = std.debug.assert;
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
                break :blk socket.connect(rt) catch {
                    socket.close_blocking();
                    continue;
                };
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

    fn send_simple_query(self: *Postgres, comptime sql: []const u8, writer: anytype) !void {
        var query_msg = Message.Frontend.Query{ .query = sql };
        try query_msg.print(writer);

        _ = try self.socket.send_all(self.rt, self.wire.send_buffer.items);
        self.wire.send_buffer.clearRetainingCapacity();
    }

    fn send_extended_query(
        self: *Postgres,
        comptime sql: []const u8,
        params: anytype,
        writer: anytype,
        descriptions: []Message.FieldDescription,
        row_count: i32,
    ) !void {
        const params_info = @typeInfo(@TypeOf(params));
        const field_count = params_info.Struct.fields.len;

        var arena = std.heap.ArenaAllocator.init(self.wire.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var parse_msg = Message.Frontend.Parse{ .query = sql };
        try parse_msg.print(writer);

        var describe_msg = Message.Frontend.Describe{ .kind = .statement };
        try describe_msg.print(writer);

        try Message.Frontend.Sync.print(writer);

        _ = try self.socket.send_all(self.rt, self.wire.send_buffer.items);
        self.wire.send_buffer.clearRetainingCapacity();

        const param_types: []const Message.PgType = blk: {
            var p_types: ?[]const Message.PgType = null;

            while (true) {
                const recv_buffer = try self.wire.next_recv();
                const read = try self.socket.recv(self.rt, recv_buffer);
                self.wire.mark_recv(read);
                while (try self.wire.process_recv()) |m| switch (m) {
                    .ParameterDescription => |parameters| {
                        for (parameters) |p| {
                            log.debug("parameter: {s}", .{@tagName(p)});
                        }
                        p_types = parameters;
                    },
                    .NoData => {},
                    .RowDescription => |columns| {
                        defer self.wire.allocator.free(columns);
                        for (columns, 0..) |col, i| {
                            descriptions[i] = col;
                            descriptions[i].name = try arena_allocator.dupe(u8, col.name);
                        }
                    },
                    .ReadyForQuery => break :blk p_types.?,
                    .ErrorResponse => return error.Failed,
                    else => log.err("unexpected message: {s}", .{@tagName(m)}),
                };
            }
        };
        defer self.wire.allocator.free(param_types);
        var formats: [field_count]Message.Format = undefined;
        var parameters: [field_count]Message.Frontend.Bind.Parameter = undefined;

        try bind_params(arena_allocator, &formats, param_types, &parameters, params);

        var bind_msg = Message.Frontend.Bind{
            .formats = &formats,
            .parameters = &parameters,
        };
        try bind_msg.print(writer);

        var execute_msg = Message.Frontend.Execute{ .row_count = row_count };
        try execute_msg.print(writer);

        var close_msg = Message.Frontend.Close{ .kind = .portal };
        try close_msg.print(writer);

        try Message.Frontend.Sync.print(writer);
        _ = try self.socket.send_all(self.rt, self.wire.send_buffer.items);
        self.wire.send_buffer.clearRetainingCapacity();
    }

    pub fn execute(self: *Postgres, comptime sql: []const u8, params: anytype) !void {
        const params_info = @typeInfo(@TypeOf(params));
        if (params_info != .Struct) @compileError("params must be a struct");
        const struct_info = params_info.Struct;
        const field_count = struct_info.fields.len;

        const writer = self.wire.send_buffer.writer(self.wire.allocator);

        if (field_count == 0) {
            try self.send_simple_query(sql, writer);

            while (true) {
                const recv_buffer = try self.wire.next_recv();
                const read = try self.socket.recv(self.rt, recv_buffer);
                self.wire.mark_recv(read);
                while (try self.wire.process_recv()) |m| switch (m) {
                    .ReadyForQuery => return,
                    .ErrorResponse => return error.ConnectionFailed,
                    .NoticeResponse, .EmptyQueryResponse => continue,
                    .CopyInResponse, .CopyOutResponse => continue,
                    .CommandComplete => |tag| log.info("command complete: {s}", .{tag}),
                    .ParameterStatus => |inner| log.info(
                        "parameter: {s}={s}",
                        .{ inner.name, inner.value },
                    ),
                    // If we returned rows, just free them.
                    .RowDescription => |columns| self.wire.allocator.free(columns),
                    .DataRow => |columns| self.wire.allocator.free(columns),
                    else => log.err("unexpected message: {s}", .{@tagName(m)}),
                };
            }
        } else {
            try self.send_extended_query(sql, params, writer, &.{}, 0);

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

    pub fn fetch_optional(
        self: *Postgres,
        allocator: std.mem.Allocator,
        comptime T: type,
        comptime sql: []const u8,
        params: anytype,
    ) !?T {
        const params_info = @typeInfo(@TypeOf(params));
        if (params_info != .Struct) @compileError("params must be a struct");
        const struct_info = params_info.Struct;
        const field_count = struct_info.fields.len;

        const writer = self.wire.send_buffer.writer(self.wire.allocator);
        var descriptions: [@typeInfo(T).Struct.fields.len]Message.FieldDescription = undefined;
        var result: ?T = null;

        if (field_count == 0) {
            try self.send_simple_query(sql, writer);

            var got_row: bool = false;
            while (true) {
                const recv_buffer = try self.wire.next_recv();
                const read = try self.socket.recv(self.rt, recv_buffer);
                self.wire.mark_recv(read);
                while (try self.wire.process_recv()) |m| switch (m) {
                    .ReadyForQuery => return result,
                    .ErrorResponse => return error.ConnectionFailed,
                    .NoticeResponse, .EmptyQueryResponse => continue,
                    .CopyInResponse, .CopyOutResponse => continue,
                    .CommandComplete => |tag| log.info("command complete: {s}", .{tag}),
                    .ParameterStatus => |inner| log.info(
                        "parameter: {s}={s}",
                        .{ inner.name, inner.value },
                    ),
                    // If we returned rows, just free them.
                    .RowDescription => |columns| {
                        defer self.wire.allocator.free(columns);
                        std.mem.copyForwards(Message.FieldDescription, descriptions[0..], columns);
                    },
                    .DataRow => |columns| {
                        defer self.wire.allocator.free(columns);
                        if (got_row) continue;
                        got_row = true;
                        result = try parse_struct(allocator, T, &descriptions, columns);
                    },
                    else => log.err("unexpected message: {s}", .{@tagName(m)}),
                };
            }
        } else {
            try self.send_extended_query(sql, params, writer, &descriptions, 1);

            while (true) {
                const recv_buffer = try self.wire.next_recv();
                const read = try self.socket.recv(self.rt, recv_buffer);
                self.wire.mark_recv(read);
                while (try self.wire.process_recv()) |m| switch (m) {
                    .ReadyForQuery => return result,
                    .ErrorResponse => return error.Failed,
                    .DataRow => |columns| {
                        defer self.wire.allocator.free(columns);
                        result = try parse_struct(allocator, T, &descriptions, columns);
                    },
                    else => log.err("unexpected message: {s}", .{@tagName(m)}),
                };
            }
        }
    }

    pub fn fetch_one(
        self: *Postgres,
        allocator: std.mem.Allocator,
        comptime T: type,
        comptime sql: []const u8,
        params: anytype,
    ) !T {
        return try self.fetch_optional(allocator, T, sql, params) orelse return error.NotFound;
    }

    pub fn fetch_all(
        self: *Postgres,
        allocator: std.mem.Allocator,
        comptime T: type,
        comptime sql: []const u8,
        params: anytype,
    ) ![]const T {
        const params_info = @typeInfo(@TypeOf(params));
        if (params_info != .Struct) @compileError("params must be a struct");
        const struct_info = params_info.Struct;
        const field_count = struct_info.fields.len;

        const writer = self.wire.send_buffer.writer(self.wire.allocator);
        var descriptions: [@typeInfo(T).Struct.fields.len]Message.FieldDescription = undefined;

        var list = try std.ArrayListUnmanaged(T).initCapacity(allocator, 0);
        defer list.deinit(allocator);

        if (field_count == 0) {
            try self.send_simple_query(sql, writer);

            while (true) {
                const recv_buffer = try self.wire.next_recv();
                const read = try self.socket.recv(self.rt, recv_buffer);
                self.wire.mark_recv(read);
                while (try self.wire.process_recv()) |m| switch (m) {
                    .ReadyForQuery => return list.toOwnedSlice(allocator),
                    .ErrorResponse => return error.ConnectionFailed,
                    .NoticeResponse, .EmptyQueryResponse => continue,
                    .CopyInResponse, .CopyOutResponse => continue,
                    .CommandComplete => |tag| log.info("command complete: {s}", .{tag}),
                    .ParameterStatus => |inner| log.info(
                        "parameter: {s}={s}",
                        .{ inner.name, inner.value },
                    ),
                    // If we returned rows, just free them.
                    .RowDescription => |columns| {
                        defer self.wire.allocator.free(columns);
                        std.mem.copyForwards(Message.FieldDescription, descriptions[0..], columns);
                    },
                    .DataRow => |columns| {
                        defer self.wire.allocator.free(columns);
                        try list.append(allocator, try parse_struct(allocator, T, &descriptions, columns));
                    },
                    else => log.err("unexpected message: {s}", .{@tagName(m)}),
                };
            }
        } else {
            try self.send_extended_query(sql, params, writer, &descriptions, 0);

            while (true) {
                const recv_buffer = try self.wire.next_recv();
                const read = try self.socket.recv(self.rt, recv_buffer);
                self.wire.mark_recv(read);
                while (try self.wire.process_recv()) |m| switch (m) {
                    .ReadyForQuery => return list.toOwnedSlice(allocator),
                    .ErrorResponse => return error.Failed,
                    .DataRow => |columns| {
                        defer self.wire.allocator.free(columns);
                        try list.append(try parse_struct(allocator, T, &descriptions, columns));
                    },
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

fn bind_float(
    allocator: std.mem.Allocator,
    comptime F: type,
    value: anytype,
    format: *Message.Format,
) !Message.Frontend.Bind.Parameter {
    format.* = .binary;
    const buf = try allocator.alloc(u8, @sizeOf(F));
    const casted_value: F = @floatCast(value);
    switch (builtin.cpu.arch.endian()) {
        .big => std.mem.copyForwards(u8, buf, std.mem.asBytes(&casted_value)),
        .little => {
            const bytes = std.mem.asBytes(&casted_value);
            for (0..buf.len) |i| {
                buf[buf.len - 1 - i] = bytes[i];
            }
        },
    }

    return .{ .length = @sizeOf(F), .value = buf };
}

fn bind_int(
    allocator: std.mem.Allocator,
    comptime I: type,
    value: anytype,
    format: *Message.Format,
) !Message.Frontend.Bind.Parameter {
    format.* = .binary;
    const buf = try allocator.alloc(u8, @sizeOf(I));
    std.mem.writeInt(I, buf[0..@sizeOf(I)], @intCast(value), .big);
    return .{ .length = @sizeOf(I), .value = buf };
}

fn bind_param(
    allocator: std.mem.Allocator,
    value: anytype,
    format: *Message.Format,
    pg_type: Message.PgType,
) !Message.Frontend.Bind.Parameter {
    const T = @TypeOf(value);
    const t_info = @typeInfo(T);

    switch (t_info) {
        .Int, .ComptimeInt => {
            return switch (pg_type) {
                .int2 => bind_int(allocator, i16, value, format),
                .int4 => bind_int(allocator, i32, value, format),
                .int8 => bind_int(allocator, i64, value, format),
                else => @panic("trying to bind unsupported type to int: " ++ @typeName(T)),
            };
        },
        .Float, .ComptimeFloat => {
            return switch (pg_type) {
                .float4 => bind_float(allocator, f32, value, format),
                .float8 => bind_float(allocator, f64, value, format),
                else => @panic("trying to bind unsupported type to float: " ++ @typeName(T)),
            };
        },
        .Optional => |_| if (value) |v| {
            return try bind_param(allocator, v, format, pg_type);
        } else {
            format.* = .binary;
            return .{ .length = -1, .value = "" };
        },
        .Null => {
            format.* = .binary;
            return .{ .length = -1, .value = "" };
        },
        .Pointer => |ptr_info| switch (ptr_info.size) {
            .Slice => switch (ptr_info.child) {
                u8 => switch (pg_type) {
                    .text => {
                        format.* = .text;
                        const buf = try allocator.dupe(u8, value);
                        return .{ .length = value.len, .value = buf };
                    },
                    .bytea => {
                        format.* = .binary;
                        const buf = try allocator.dupe(u8, value);
                        return .{ .length = value.len, .value = buf };
                    },
                    else => @panic("trying to bind unsupported slice type: " ++ @typeName(T)),
                },
                else => @compileError("unsupported slice type: " ++ @typeName(T)),
            },
            .One => return try bind_param(allocator, value.*, format, pg_type),
            else => @compileError("unsupported pointer type: " ++ @typeName(T)),
        },
        .Array => |info| switch (info.child) {
            u8 => switch (pg_type) {
                .text => {
                    format.* = .text;
                    const buf = try allocator.dupe(u8, &value);
                    return .{ .length = value.len, .value = buf };
                },
                .bytea => {
                    format.* = .binary;
                    const buf = try allocator.dupe(u8, &value);
                    return .{ .length = value.len, .value = buf };
                },
                else => @panic("trying to bind unsupported array type: " ++ @typeName(T)),
            },
            else => @compileError("unsupported array type: " ++ @typeName(T)),
        },
        .Bool => {
            if (pg_type == .bool) {
                format.* = .binary;
                const buf = try allocator.alloc(u8, 1);
                buf[0] = @intFromBool(value);
                return .{ .length = 1, .value = buf };
            } else @panic("trying to bind unsupported type to bool: " ++ @typeName(T));
        },
        else => @compileError("unsupported type for postgres binding: " ++ @typeName(T)),
    }
}

fn bind_params(
    allocator: std.mem.Allocator,
    formats: []Message.Format,
    param_types: []const Message.PgType,
    parameters: []Message.Frontend.Bind.Parameter,
    params: anytype,
) !void {
    assert(param_types.len == formats.len);
    assert(param_types.len == parameters.len);

    const params_info = @typeInfo(@TypeOf(params));
    if (params_info != .Struct) @compileError("params must be a tuple or struct");

    inline for (params_info.Struct.fields, 0..) |field, i| {
        const value = @field(params, field.name);
        parameters[i] = try bind_param(allocator, value, &formats[i], param_types[i]);
    }
}

fn parse_int(comptime I: type, format: Message.Format, bytes: []const u8) !I {
    return switch (format) {
        .text => try std.fmt.parseInt(I, bytes, 10),
        .binary => @intCast(std.mem.readInt(I, bytes[0..@sizeOf(I)], .big)),
    };
}

fn parse_float(comptime F: type, format: Message.Format, bytes: []const u8) !F {
    return switch (format) {
        .text => try std.fmt.parseFloat(F, bytes),
        .binary => switch (builtin.cpu.arch.endian()) {
            .big => std.mem.bytesToValue(F, bytes[0..@sizeOf(F)]),
            .little => blk: {
                var buf: [@sizeOf(F)]u8 = undefined;
                for (0..bytes.len) |i| {
                    buf[i] = bytes[bytes.len - 1 - i];
                }
                break :blk std.mem.bytesToValue(F, bytes[0..@sizeOf(F)]);
            },
        },
    };
}

fn parse_field(
    allocator: std.mem.Allocator,
    comptime T: type,
    description: Message.FieldDescription,
    column: []const u8,
) !T {
    return switch (@typeInfo(T)) {
        .Int => switch (description.pg_type) {
            .int2 => @as(T, @intCast(try parse_int(i16, description.format, column))),
            .int4 => @as(T, @intCast(try parse_int(i32, description.format, column))),
            .int8 => @as(T, @intCast(try parse_int(i64, description.format, column))),
            else => return error.MismatchedTypes,
        },
        .Float => switch (description.pg_type) {
            .float4 => @as(T, @floatCast(try parse_float(f32, description.format, column))),
            .float8 => @as(T, @floatCast(try parse_float(f64, description.format, column))),
            else => return error.MismatchedTypes,
        },
        .Optional => |info| blk: {
            break :blk @as(T, try parse_field(allocator, info.child, description, column));
        },
        else => switch (T) {
            []const u8, []u8 => try allocator.dupe(u8, column),
            else => @compileError("Unsupported type for pg columning: " ++ @typeName(T)),
        },
    };
}

fn parse_struct(
    allocator: std.mem.Allocator,
    comptime T: type,
    descriptions: []const Message.FieldDescription,
    columns: []const ?[]const u8,
) !T {
    var result: T = undefined;

    const struct_info = @typeInfo(T);
    if (struct_info != .Struct) @compileError("item being parsed must be a struct");
    const struct_fields = struct_info.Struct.fields;
    var set_fields: [struct_fields.len]u1 = .{0} ** struct_fields.len;

    inline for (struct_fields, 0..) |field, i| {
        if (field.default_value) |default| {
            @field(result, field.name) = @as(*const field.type, @ptrCast(@alignCast(default))).*;
            set_fields[i] = 1;
        } else if (@typeInfo(field.type) == .Optional) {
            @field(result, field.name) = null;
            set_fields[i] = 1;
        }

        var desc_number: usize = 0;
        const desc: ?Message.FieldDescription = blk: {
            for (descriptions, 0..) |desc, j| {
                if (std.mem.eql(u8, desc.name, field.name)) {
                    desc_number = j;
                    break :blk desc;
                }
            }

            break :blk null;
        };

        if (columns[desc_number]) |c| if (desc) |d| {
            @field(result, field.name) = try parse_field(allocator, field.type, d, c);
            set_fields[i] = 1;
        };
    }

    inline for (set_fields[0..], 0..) |set, i| if (set == 0) {
        log.err("missing required field: {s}", .{struct_fields[i].name});
        return error.MissingRequiredField;
    };

    return result;
}
