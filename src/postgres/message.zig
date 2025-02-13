const std = @import("std");
const log = std.log.scoped(.@"sqlt/postgres/message");

pub fn Pair(comptime A: type, comptime B: type) type {
    return struct { A, B };
}

pub const StartupMessage = struct {
    pub const Builder = struct {
        allocator: std.mem.Allocator,
        pairs: std.ArrayListUnmanaged([2][]const u8),

        pub fn init(allocator: std.mem.Allocator) !Builder {
            return .{
                .allocator = allocator,
                .pairs = try std.ArrayListUnmanaged([2][]const u8).initCapacity(allocator, 0),
            };
        }

        pub fn add_parameter(self: *Builder, pair: [2][]const u8) !void {
            try self.pairs.append(self.allocator, pair);
        }

        pub fn build(self: *Builder) !StartupMessage {
            defer self.pairs.deinit(self.allocator);
            const pairs = try self.pairs.toOwnedSlice(self.allocator);

            var total_size: usize = @sizeOf(i32) * 2;
            for (pairs) |pair| {
                total_size += pair[0].len + 1;
                total_size += pair[1].len + 1;
            }

            return StartupMessage{ .length = @intCast(total_size + 1), .payload = pairs };
        }
    };

    length: i32,
    protocol: i32 = 196608,
    payload: []const [2][]const u8,

    pub fn write(self: StartupMessage, writer: anytype) !void {
        try writer.writeInt(i32, self.length, .big);
        try writer.writeInt(i32, self.protocol, .big);

        for (self.payload) |pair| {
            try writer.writeAll(pair[0]);
            try writer.writeByte(0);
            try writer.writeAll(pair[1]);
            try writer.writeByte(0);
        }

        try writer.writeByte(0);
    }
};

// This will likely become just the BackendMessages
// and there will be some other interface for FrontendMessages.
pub const Message = union(enum) {
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

    BindComplete,
    CloseComplete,
    CommandComplete,

    // None of these Copy stuff is supported right now.
    CopyData: struct { data: []const u8 },
    CopyDone,
    CopyInResponse: struct { format: i8, column_count: i16, formats: []i16 },
    CopyOutResponse: struct { format: i8, column_count: i16, formats: []i16 },
    CopyBothRespone: struct { format: i8, column_count: i16, formats: []i16 },

    DataRow: struct { columns: []const ?[]const u8 },
    EmptyQueryResponse,

    ErrorResponse,
    BackendKeyData,
    ReadyForQuery,

    Unknown,

    ParameterStatus: struct {
        name: []const u8,
        value: []const u8,
    },

    pub fn parse(allocator: std.mem.Allocator, ident: u8, payload: []const u8) !Message {
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
            'B' => .BindComplete,
            '3' => .CloseComplete,
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
            'S' => blk: {
                const name_end = std.mem.indexOfScalar(u8, payload, 0) orelse return error.InvalidMessage;
                const value_start = name_end + 1;
                var value_end = std.mem.indexOfScalar(
                    u8,
                    payload[value_start..],
                    0,
                ) orelse return error.InvalidMessage;
                value_end += value_start;

                break :blk Message{ .ParameterStatus = .{
                    .name = payload[0..name_end],
                    .value = payload[value_start..value_end],
                } };
            },
            'Z' => .ReadyForQuery,
            else => blk: {
                log.warn("got message with ident: {d}", .{ident});
                break :blk .Unknown;
            },
        };
    }
};
