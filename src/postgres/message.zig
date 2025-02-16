const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"sqlt/postgres/message");

// https://www.postgresql.org/docs/current/protocol-message-formats.html
pub const Message = struct {
    pub const Format = enum(i16) { text = 0, binary = 1 };

    pub const PgType = enum(i32) {
        bool = 16,
        bytea = 17,
        char = 18,
        int8 = 20,
        int2 = 21,
        int4 = 23,
        text = 25,
        float4 = 700,
        float8 = 701,
    };

    pub const FieldDescription = struct {
        name: []const u8,
        table_oid: i32,
        col_number: i16,
        pg_type: Message.PgType,
        pg_type_size: i16,
        pg_type_attr: i32,
        format: Format,
    };

    /// For Backend Messages.
    pub const Backend = union(enum) {
        // Using the pre-existing Postgres Naming Convention
        AuthenticationOk,
        AuthenticationKerberosV5,
        AuthenticationCleartextPassword,
        AuthenticationMD5Password: struct { salt: [4]u8 },
        AuthenticationGSS,
        AuthenticationGSSContinue: struct { data: []const u8 },
        AuthenticationSSPI,
        AuthenticationSASL: struct { name: []const u8 },
        AuthenticationSASLContinue: struct { data: []const u8 },
        AuthenticationSASLFinal: struct { data: []const u8 },

        BackendKeyData,
        BindComplete,
        CloseComplete,
        CommandComplete: []const u8,

        // None of these Copy stuff is supported right now.
        CopyData: struct { data: []const u8 },
        CopyDone,
        CopyInResponse: struct { format: i8, column_count: i16, formats: []i16 },
        CopyOutResponse: struct { format: i8, column_count: i16, formats: []i16 },
        CopyBothRespone: struct { format: i8, column_count: i16, formats: []i16 },

        DataRow: []const ?[]const u8,
        EmptyQueryResponse,

        // TODO: https://www.postgresql.org/docs/current/protocol-error-fields.html
        ErrorResponse,

        FunctionCallResponse: struct { data: []const u8 },
        NegotiateProtocolVersion: struct { minor: i32, options: []const []const u8 },
        NoData,
        // TODO: add fields
        NoticeResponse,
        NotificationResponse: struct { process_id: i32, name: []const u8, payload: []const u8 },
        ParameterDescription: []const PgType,
        ParameterStatus: struct { name: []const u8, value: []const u8 },
        ParseComplete,
        PortalSuspended,
        ReadyForQuery,
        RowDescription: []const FieldDescription,

        Unknown,

        pub fn parse(allocator: std.mem.Allocator, ident: u8, payload: []const u8) !Message.Backend {
            return switch (ident) {
                'R' => blk: {
                    const auth_type = std.mem.readInt(i32, payload[0..4], .big);
                    const auth_data = payload[4..];
                    break :blk switch (auth_type) {
                        0 => .AuthenticationOk,
                        2 => .AuthenticationKerberosV5,
                        3 => .AuthenticationCleartextPassword,
                        5 => .{ .AuthenticationMD5Password = .{
                            .salt = .{ auth_data[0], auth_data[1], auth_data[2], auth_data[3] },
                        } },
                        7 => .{ .AuthenticationGSSContinue = .{ .data = auth_data } },
                        9 => .AuthenticationSSPI,
                        10 => .{ .AuthenticationSASL = .{ .name = auth_data } },
                        11 => .{ .AuthenticationSASLContinue = .{ .data = auth_data } },
                        12 => .{ .AuthenticationSASLFinal = .{ .data = auth_data } },
                        else => return error.InvalidAuthenticationMessage,
                    };
                },
                'K' => .BackendKeyData,
                '2' => .BindComplete,
                '3' => .CloseComplete,
                'C' => .{ .CommandComplete = payload },
                'D' => blk: {
                    const column_count = std.mem.readInt(i16, payload[0..2], .big);
                    const column_count_usize: usize = @intCast(column_count);

                    var values = try allocator.alloc(?[]const u8, column_count_usize);
                    errdefer allocator.free(values);

                    var column_pos: usize = 2;
                    for (0..column_count_usize) |i| {
                        if (column_pos + 4 > payload.len) return error.InvalidMessage;
                        const length = std.mem.readInt(i32, payload[column_pos..][0..4], .big);
                        column_pos += 4;

                        if (length == -1) {
                            values[i] = null;
                        } else {
                            const length_usize: usize = @intCast(length);
                            if (column_pos + length_usize > payload.len) return error.InvalidMessage;
                            values[i] = payload[column_pos .. column_pos + length_usize];
                            column_pos += length_usize;
                        }
                    }

                    break :blk .{ .DataRow = values };
                },
                'I' => .EmptyQueryResponse,
                'E' => .ErrorResponse,
                'n' => .NoData,
                'N' => .NoticeResponse,
                't' => blk: {
                    const param_count = std.mem.readInt(i16, payload[0..2], .big);
                    const param_count_usize: usize = @intCast(param_count);

                    var values = try allocator.alloc(PgType, param_count_usize);
                    errdefer allocator.free(values);

                    var column_pos: usize = 2;
                    for (0..param_count_usize) |i| {
                        const oid = std.mem.readInt(i32, payload[column_pos..][0..@sizeOf(i32)], .big);
                        column_pos += @sizeOf(i32);
                        values[i] = std.meta.intToEnum(PgType, oid) catch {
                            log.err("unrecognized oid: {d}", .{oid});
                            return error.UnknownPgType;
                        };
                    }

                    break :blk .{ .ParameterDescription = values };
                },
                'S' => blk: {
                    const name_end = std.mem.indexOfScalar(u8, payload, 0) orelse return error.InvalidMessage;
                    const value_start = name_end + 1;
                    var value_end = std.mem.indexOfScalar(
                        u8,
                        payload[value_start..],
                        0,
                    ) orelse return error.InvalidMessage;
                    value_end += value_start;

                    break :blk .{ .ParameterStatus = .{
                        .name = payload[0..name_end],
                        .value = payload[value_start..value_end],
                    } };
                },
                '1' => .ParseComplete,
                's' => .PortalSuspended,
                'Z' => .ReadyForQuery,
                'T' => blk: {
                    const column_count = std.mem.readInt(i16, payload[0..2], .big);
                    const column_count_usize: usize = @intCast(column_count);

                    var values = try allocator.alloc(FieldDescription, column_count_usize);
                    errdefer allocator.free(values);

                    var column_pos: usize = 2;
                    for (0..column_count_usize) |i| {
                        const value_ptr = &values[i];

                        const sentinel = std.mem.indexOfSentinel(u8, 0, @ptrCast(&payload[column_pos]));
                        value_ptr.name = payload[column_pos..][0..sentinel];
                        column_pos += value_ptr.name.len + 1;

                        value_ptr.table_oid = std.mem.readInt(i32, payload[column_pos..][0..4], .big);
                        column_pos += 4;

                        value_ptr.col_number = std.mem.readInt(i16, payload[column_pos..][0..2], .big);
                        column_pos += 2;

                        const type_oid = std.mem.readInt(i32, payload[column_pos..][0..4], .big);
                        value_ptr.pg_type = std.meta.intToEnum(PgType, type_oid) catch {
                            log.err("unrecognized oid: {d}", .{type_oid});
                            return error.UnknownPgType;
                        };
                        column_pos += 4;

                        value_ptr.pg_type_size = std.mem.readInt(i16, payload[column_pos..][0..2], .big);
                        column_pos += 2;

                        value_ptr.pg_type_attr = std.mem.readInt(i32, payload[column_pos..][0..4], .big);
                        column_pos += 4;

                        const format_int = std.mem.readInt(i16, payload[column_pos..][0..2], .big);
                        value_ptr.format = std.meta.intToEnum(Format, format_int) catch {
                            log.err("unrecognized format: {d}", .{format_int});
                            return error.UnknownFormat;
                        };
                        column_pos += 2;
                    }

                    break :blk .{ .RowDescription = values };
                },
                else => blk: {
                    log.warn("got message with ident: {d}", .{ident});
                    break :blk .Unknown;
                },
            };
        }
    };

    pub const Frontend = struct {
        pub const StartupMessage = struct {
            const Self = @This();
            length: i32 = std.math.maxInt(i32),
            protocol: i32 = 196608,
            pairs: []const [2][]const u8,

            pub fn print(self: *Self, writer: anytype) !void {
                var length: usize = @sizeOf(i32) * 2;
                for (self.pairs) |pair| length += (pair[0].len + 1 + pair[1].len + 1);
                length += 1;
                self.length = @intCast(length);
                assert(self.length != std.math.maxInt(i32));

                try writer.writeInt(i32, self.length, .big);
                try writer.writeInt(i32, self.protocol, .big);
                for (self.pairs) |pair| {
                    try writer.writeAll(pair[0]);
                    try writer.writeByte(0);
                    try writer.writeAll(pair[1]);
                    try writer.writeByte(0);
                }
                try writer.writeByte(0);
            }
        };

        pub const Query = struct {
            const Self = @This();
            ident: u8 = 'Q',
            query: []const u8,

            pub fn print(self: *Self, writer: anytype) !void {
                var length: usize = @sizeOf(i32);
                length += self.query.len + 1;

                try writer.writeByte(self.ident);
                try writer.writeInt(i32, @intCast(length), .big);
                try writer.writeAll(self.query);
                try writer.writeByte(0);
            }
        };

        pub const Describe = struct {
            const Self = @This();
            ident: u8 = 'D',
            kind: enum(u8) { statement = 'S', portal = 'P' },
            name: []const u8 = "",

            pub fn print(self: *Self, writer: anytype) !void {
                var length: usize = @sizeOf(i32);
                length += @sizeOf(u8);
                length += self.name.len + 1;

                try writer.writeByte(self.ident);
                try writer.writeInt(i32, @intCast(length), .big);
                try writer.writeByte(@intFromEnum(self.kind));
                try writer.writeAll(self.name);
                try writer.writeByte(0);
            }
        };

        pub const Parse = struct {
            const Self = @This();
            ident: u8 = 'P',
            name_prepared: []const u8 = "",
            query: []const u8,
            parameter_types: []const i32 = &.{},

            pub fn print(self: *Self, writer: anytype) !void {
                var length: usize = @sizeOf(i32);
                length += self.name_prepared.len + 1;
                length += self.query.len + 1;
                // omitted: parameter_type_count
                length += @sizeOf(i16);
                length += self.parameter_types.len * @sizeOf(i32);

                try writer.writeByte(self.ident);
                try writer.writeInt(i32, @intCast(length), .big);
                try writer.writeAll(self.name_prepared);
                try writer.writeByte(0);
                try writer.writeAll(self.query);
                try writer.writeByte(0);
                try writer.writeInt(i16, @intCast(self.parameter_types.len), .big);
                for (self.parameter_types) |pt| {
                    try writer.writeInt(i32, pt, .big);
                }
            }
        };

        pub const Bind = struct {
            pub const Parameter = struct {
                length: i32,
                value: []const u8,
            };

            const Self = @This();
            ident: u8 = 'B',
            name_portal: []const u8 = "",
            name_prepared: []const u8 = "",
            formats: []const Format,
            parameters: []const Parameter,
            results: []const Format = &.{},

            pub fn print(self: *Self, writer: anytype) !void {
                var length: usize = @sizeOf(i32);
                length += self.name_portal.len + 1;
                length += self.name_prepared.len + 1;
                // omitted: format_count
                length += @sizeOf(i16);
                length += self.formats.len * @sizeOf(i16);
                // omitted: parameter_count
                length += @sizeOf(i16);
                for (self.parameters) |p| length += @sizeOf(i32) + p.value.len;
                // omitted: result_count
                length += @sizeOf(i16);
                length += @sizeOf(i16) * self.results.len;

                try writer.writeByte(self.ident);
                try writer.writeInt(i32, @intCast(length), .big);
                try writer.writeAll(self.name_portal);
                try writer.writeByte(0);
                try writer.writeAll(self.name_prepared);
                try writer.writeByte(0);
                try writer.writeInt(i16, @intCast(self.formats.len), .big);
                for (self.formats) |f| try writer.writeInt(i16, @intFromEnum(f), .big);
                try writer.writeInt(i16, @intCast(self.parameters.len), .big);
                for (self.parameters) |p| {
                    try writer.writeInt(i32, p.length, .big);
                    try writer.writeAll(p.value);
                }
                try writer.writeInt(i16, @intCast(self.results.len), .big);
                for (self.results) |r| try writer.writeInt(i16, @intFromEnum(r), .big);
            }
        };

        pub const Execute = struct {
            const Self = @This();
            ident: u8 = 'E',
            name: []const u8 = "",
            row_count: i32,

            pub fn print(self: *Self, writer: anytype) !void {
                var length: usize = @sizeOf(i32);
                length += self.name.len + 1;
                length += @sizeOf(i32);

                try writer.writeByte(self.ident);
                try writer.writeInt(i32, @intCast(length), .big);
                try writer.writeAll(self.name);
                try writer.writeByte(0);
                try writer.writeInt(i32, self.row_count, .big);
            }
        };

        pub const Sync = struct {
            pub fn print(writer: anytype) !void {
                try writer.writeByte('S');
                try writer.writeInt(i32, @sizeOf(i32), .big);
            }
        };

        pub const Close = struct {
            const Self = @This();
            ident: u8 = 'C',
            kind: enum(u8) { statement = 'S', portal = 'P' },
            name: []const u8 = "",

            pub fn print(self: *Self, writer: anytype) !void {
                var length: usize = @sizeOf(i32);
                length += @sizeOf(u8);
                length += self.name.len + 1;

                try writer.writeByte(self.ident);
                try writer.writeInt(i32, @intCast(length), .big);
                try writer.writeByte(@intFromEnum(self.kind));
                try writer.writeAll(self.name);
                try writer.writeByte(0);
            }
        };
    };
};
