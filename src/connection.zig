const std = @import("std");

const Row = struct {
    inner: *anyopaque,
};

pub const Connection = struct {
    inner: *anyopaque,
    vtable: VTable,

    const VTable = struct {
        connect: *const fn ([]const u8) anyerror!Connection,
        execute: *const fn (*anyopaque, comptime []const u8, anytype) anyerror!void,
        fetch_row: *const fn (*anyopaque, comptime []const u8, anytype) anyerror!?Row,
    };

    pub fn fetch_one(
        self: Connection,
        comptime T: type,
        comptime sql: []const u8,
        params: anytype,
    ) !T {
        const maybe_row = try self.vtable.fetch_row(self.inner, sql, params);
        const row = maybe_row orelse return error.NoRows;

        var result: T = undefined;
        inline for (std.meta.fields(T)) |field| {
            @field(result, field.name) = try row.get(field.type, field.name);
        }
        return result;
    }

    //pub fn fetch_optional(
    //    self: Connection,
    //    comptime T: type,
    //    comptime sql: []const u8,
    //    params: anytype,
    //) !?T {}

    //pub fn fetch_all(
    //    self: Connection,
    //    allocator: std.mem.Allocator,
    //    comptime T: type,
    //    comptime sql: []const u8,
    //    params: anytype,
    //) ![]T {}
};
