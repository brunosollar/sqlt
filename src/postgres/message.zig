const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"sqlt/postgres/message");

pub fn Pair(comptime A: type, comptime B: type) type {
    return struct { A, B };
}

const FieldDescription = struct {
    name: []const u8,
    table_oid: i32,
    attr_num: i16,
    oid: i32,
    typlen: i16,
    atttypmod: i32,
    format: i16,
};

// https://www.postgresql.org/docs/current/protocol-message-formats.html
pub const Message = struct {
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

        DataRow: struct { columns: []const ?[]const u8 },
        EmptyQueryResponse,

        // TODO: https://www.postgresql.org/docs/current/protocol-error-fields.html
        ErrorResponse,

        FunctionCallResponse: struct { data: []const u8 },
        NegotiateProtocolVersion: struct { minor: i32, options: []const []const u8 },
        NoData,
        // TODO: add fields
        NoticeResponse,
        NotificationResponse: struct { process_id: i32, name: []const u8, payload: []const u8 },
        ParameterDescription: struct { parameters: []const i32 },
        ParameterStatus: struct { name: []const u8, value: []const u8 },
        ParseComplete,
        PortalSuspended,
        ReadyForQuery,
        RowDescription: struct { columns: []const FieldDescription },

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

                    break :blk .{ .DataRow = .{ .columns = values } };
                },
                'I' => .EmptyQueryResponse,
                'E' => .ErrorResponse,
                'n' => .NoData,
                'N' => .NoticeResponse,
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
                'Z' => .ReadyForQuery,
                'B' => blk: {
                    const column_count = std.mem.readInt(i16, payload[0..2], .big);
                    const column_count_usize: usize = @intCast(column_count);

                    var values = try allocator.alloc(FieldDescription, column_count_usize);
                    errdefer allocator.free(values);

                    var column_pos: usize = 2;
                    for (0..column_count_usize) |i| {
                        const value_ptr = &values[i];

                        const sentinel = std.mem.indexOfSentinel(u8, 0, @ptrCast(&payload[column_pos]));
                        value_ptr.name = payload[column_pos..][0..sentinel];
                        column_pos = sentinel;

                        value_ptr.table_oid = std.mem.readInt(i32, payload[column_pos..][0..4], .big);
                        column_pos += 4;

                        value_ptr.attr_num = std.mem.readInt(i16, payload[column_pos..][0..2], .big);
                        column_pos += 2;

                        value_ptr.oid = std.mem.readInt(i32, payload[column_pos..][0..4], .big);
                        column_pos += 4;

                        value_ptr.typlen = std.mem.readInt(i16, payload[column_pos..][0..2], .big);
                        column_pos += 2;

                        value_ptr.atttypmod = std.mem.readInt(i32, payload[column_pos..][0..4], .big);
                        column_pos += 4;

                        value_ptr.format = std.mem.readInt(i16, payload[column_pos..][0..2], .big);
                        column_pos += 2;
                    }

                    break :blk .{ .RowDescription = .{ .columns = values } };
                },
                else => blk: {
                    log.warn("got message with ident: {d}", .{ident});
                    break :blk .Unknown;
                },
            };
        }
    };

    pub const Frontend = struct {
        fn finalize_inner(self: anytype) usize {
            const T = @TypeOf(self);

            return switch (@typeInfo(T)) {
                .Int => @sizeOf(T),
                .Array => |_| blk: {
                    var sub_length: usize = 0;
                    for (self) |item| sub_length += finalize_inner(item);
                    break :blk sub_length;
                },
                .Pointer => |ptr_info| switch (ptr_info.size) {
                    .Slice => blk: {
                        var sub_length: usize = 0;
                        for (self) |item| sub_length += finalize_inner(item);
                        break :blk sub_length;
                    },
                    else => @compileError("unsupported pointer type"),
                },
                else => @panic("Unsupported type:" ++ @typeName(self) ++ " | " ++ @tagName(@typeInfo(T))),
            };
        }

        fn finalize(self: anytype) void {
            const P = @TypeOf(self);
            const parent_ptr_info = @typeInfo(P).Pointer;

            const T = parent_ptr_info.child;
            if (@typeInfo(T) != .Struct) @compileError("can only finalize a struct");
            const struct_info = @typeInfo(T).Struct;

            var length: usize = 0;
            inline for (struct_info.fields) |field| {
                if (comptime std.mem.eql(u8, field.name, "ident")) continue;
                length += finalize_inner(@field(self, field.name));
                log.debug("on field: {s} | length={d}", .{ field.name, length });
            }

            self.length = @intCast(length);
        }

        pub const StartupMessage = struct {
            const Self = @This();
            length: i32 = std.math.maxInt(i32),
            protocol: i32 = 196608,
            pairs: []const [2][]const u8,

            pub fn print(self: *Self, writer: anytype) !void {
                finalize(self);
                // Do we want to allow length adjustments?
                for (self.pairs) |_| self.length += 2;
                self.length += 1;

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
            length: i32 = std.math.maxInt(i32),
            query: []const u8,

            pub fn print(self: *Self, writer: anytype) !void {
                finalize(self);
                self.length += 1;
                assert(self.length != std.math.maxInt(i32));

                try writer.writeByte(self.ident);
                try writer.writeInt(i32, self.length, .big);
                try writer.writeAll(self.query);
                try writer.writeByte(0);
            }
        };
    };
};
