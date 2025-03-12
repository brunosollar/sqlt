const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"sqlt/sqlite");

const c = @cImport({
    @cInclude("sqlite3.h");
});

// TODO: thread-local statement cache
pub const Sqlite = struct {
    db: *c.sqlite3,

    pub fn open(path: [:0]const u8) !Sqlite {
        var sqlite: ?*c.sqlite3 = null;
        errdefer {
            if (sqlite) |sql| _ = c.sqlite3_close(sql);
        }

        const rc = c.sqlite3_open(path.ptr, &sqlite);
        if (rc != c.SQLITE_OK) {
            log.err(
                "sqlite3 open failed: {s}",
                .{std.mem.span(c.sqlite3_errmsg(sqlite))},
            );
            return error.FailedOpen;
        }

        return .{ .db = sqlite.? };
    }

    pub fn close(self: Sqlite) void {
        _ = c.sqlite3_close(self.db);
    }

    fn bind_param(
        stmt: *c.sqlite3_stmt,
        value: anytype,
        index: c_int,
    ) c_int {
        const T = @TypeOf(value);
        return switch (@typeInfo(T)) {
            .int, .comptime_int => c.sqlite3_bind_int64(stmt, index, @intCast(value)),
            .float, .comptime_float => c.sqlite3_bind_double(stmt, index, @floatCast(value)),
            .optional => |_| if (value) |v|
                bind_param(stmt, v, index)
            else
                c.sqlite3_bind_null(stmt, index),
            .null => c.sqlite3_bind_null(stmt, index),
            .pointer => |ptr_info| switch (ptr_info.size) {
                .slice => switch (ptr_info.child) {
                    u8 => c.sqlite3_bind_text(
                        stmt,
                        index,
                        value.ptr,
                        @intCast(value.len),
                        c.SQLITE_TRANSIENT,
                    ),
                    else => @compileError("Unsupported slice type: " ++ @typeName(T)),
                },
                .one => bind_param(stmt, value.*, index),
                else => @compileError("Unsupported pointer type: " ++ @typeName(T)),
            },
            .array => |info| switch (info.child) {
                u8 => c.sqlite3_bind_text(
                    stmt,
                    index,
                    &value,
                    @intCast(value.len),
                    c.SQLITE_TRANSIENT,
                ),
                else => @compileError("Unsupported array type: " ++ @typeName(T)),
            },
            .bool => c.sqlite3_bind_int(stmt, index, @intFromBool(value)),
            .@"enum" => |info| switch (@typeInfo(info.tag_type)) {
                .int, .comptime_int => bind_param(stmt, @intFromEnum(value), index),
                else => @compileError("enums must have a backing integer: " ++ @typeName(T)),
            },
            else => @compileError("Unsupported type for sqlite binding: " ++ @typeName(T)),
        };
    }

    fn bind_params(stmt: *c.sqlite3_stmt, params: anytype) !void {
        const params_info = @typeInfo(@TypeOf(params));
        if (params_info != .@"struct") @compileError("params must be a tuple or struct");

        inline for (params_info.@"struct".fields, 0..) |field, i| {
            const index: c_int = @intCast(i + 1);
            const value = @field(params, field.name);
            const rc = bind_param(stmt, value, index);
            if (rc != c.SQLITE_OK) return error.BindError;
        }
    }

    pub fn execute(self: Sqlite, comptime sql: []const u8, params: anytype) !void {
        var stmt: ?*c.sqlite3_stmt = null;

        const rc = c.sqlite3_prepare_v2(self.db, sql.ptr, @intCast(sql.len), &stmt, null);
        if (rc != c.SQLITE_OK) {
            log.err("sqlite3 prepare failed: {s}", .{c.sqlite3_errmsg(self.db)});
            return error.FailedPrepare;
        }
        defer _ = c.sqlite3_finalize(stmt);

        try bind_params(stmt.?, params);

        const step_rc = c.sqlite3_step(stmt);
        if (step_rc != c.SQLITE_DONE) {
            log.err("sqlite3 step failed: {s}", .{c.sqlite3_errmsg(self.db)});
            return error.StepError;
        }
    }

    fn parse_field(
        allocator: std.mem.Allocator,
        comptime T: type,
        comptime name: []const u8,
        index: c_int,
        stmt: *c.sqlite3_stmt,
    ) !T {
        return switch (@typeInfo(T)) {
            .int => @as(T, @intCast(c.sqlite3_column_int64(stmt, index))),
            .float => @as(T, @floatCast(c.sqlite3_column_double(stmt, index))),
            .optional => |info| blk: {
                const col_type = c.sqlite3_column_type(stmt, index);
                if (col_type == c.SQLITE_NULL) break :blk null;
                break :blk @as(T, try parse_field(allocator, info.child, name, index, stmt));
            },
            .bool => if (c.sqlite3_column_int(stmt, index) == 0) false else true,
            .@"enum" => |info| switch (@typeInfo(info.tag_type)) {
                .int, .comptime_int => @enumFromInt(c.sqlite3_column_int64(stmt, index)),
                else => @compileError("enums must have a backing integer: " ++ @typeName(T)),
            },
            else => switch (T) {
                []const u8,
                []u8,
                => try allocator.dupe(u8, std.mem.span(c.sqlite3_column_text(stmt, index))),
                else => @compileError("Unsupported type for sqlite columning: " ++ @typeName(T)),
            },
        };
    }

    fn parse_struct(allocator: std.mem.Allocator, comptime T: type, stmt: *c.sqlite3_stmt) !T {
        var result: T = undefined;

        const struct_info = @typeInfo(T);
        if (struct_info != .@"struct") @compileError("item being parsed must be a struct");
        const struct_fields = struct_info.@"struct".fields;

        const col_count: c_int = c.sqlite3_column_count(stmt);
        var set_fields: [struct_fields.len]u1 = .{0} ** struct_fields.len;

        inline for (struct_fields, 0..) |field, i| if (field.defaultValue()) |default| {
            @field(result, field.name) = default;
            set_fields[i] = 1;
        } else if (@typeInfo(field.type) == .optional) {
            @field(result, field.name) = null;
            set_fields[i] = 1;
        };

        for (0..@intCast(col_count)) |i| {
            const index: c_int = @intCast(i);
            const col_name = c.sqlite3_column_name(stmt, index);
            inline for (struct_fields, 0..) |field, j| {
                if (std.mem.eql(u8, field.name, std.mem.span(col_name))) {
                    const value = try parse_field(allocator, field.type, field.name, index, stmt);
                    @field(result, field.name) = value;
                    set_fields[j] = 1;
                }
            }
        }

        inline for (set_fields[0..], 0..) |set, i| if (set == 0) {
            log.err("missing required field: {s}", .{struct_fields[i].name});
            return error.MissingRequiredField;
        };

        return result;
    }

    pub fn fetch_optional(
        self: Sqlite,
        allocator: std.mem.Allocator,
        comptime T: type,
        comptime sql: []const u8,
        params: anytype,
    ) !?T {
        var stmt: ?*c.sqlite3_stmt = null;

        const rc = c.sqlite3_prepare_v2(self.db, sql.ptr, @intCast(sql.len), &stmt, null);
        if (rc != c.SQLITE_OK) {
            log.err("sqlite3 prepare failed: {s}", .{c.sqlite3_errmsg(self.db)});
            return error.FailedPrepare;
        }
        defer _ = c.sqlite3_finalize(stmt);

        try bind_params(stmt.?, params);

        switch (c.sqlite3_step(stmt)) {
            c.SQLITE_ROW => return try parse_struct(allocator, T, stmt.?),
            c.SQLITE_DONE => return null,
            else => unreachable,
        }
    }

    pub fn fetch_one(
        self: Sqlite,
        allocator: std.mem.Allocator,
        comptime T: type,
        comptime sql: []const u8,
        params: anytype,
    ) !T {
        return try self.fetch_optional(allocator, T, sql, params) orelse error.NotFound;
    }

    pub fn fetch_all(
        self: Sqlite,
        allocator: std.mem.Allocator,
        comptime T: type,
        comptime sql: []const u8,
        params: anytype,
    ) ![]const T {
        var stmt: ?*c.sqlite3_stmt = null;

        const rc = c.sqlite3_prepare_v2(self.db, sql.ptr, @intCast(sql.len), &stmt, null);
        if (rc != c.SQLITE_OK) {
            log.err("sqlite3 prepare failed: {s}", .{c.sqlite3_errmsg(self.db)});
            return error.FailedPrepare;
        }
        defer _ = c.sqlite3_finalize(stmt);

        try bind_params(stmt.?, params);

        var list = try std.ArrayListUnmanaged(T).initCapacity(allocator, 0);
        defer list.deinit(allocator);

        while (true) {
            switch (c.sqlite3_step(stmt)) {
                c.SQLITE_DONE => return try list.toOwnedSlice(allocator),
                c.SQLITE_ROW => try list.append(allocator, try parse_struct(allocator, T, stmt.?)),
                else => unreachable,
            }
        }
    }

    const Column = union(enum) {
        none,
        boolean: bool,
        integer: i64,
        double: f64,
        text: []const u8,
    };

    fn parse_row(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt) ![]const Column {
        var list = try std.ArrayListUnmanaged(Column).initCapacity(allocator, 0);
        errdefer for (list.items) |col| if (col == .text) allocator.free(col.text);
        defer list.deinit(allocator);

        const col_count: c_int = c.sqlite3_column_count(stmt);

        for (0..@intCast(col_count)) |i| {
            const index: c_int = @intCast(i);
            const col_type = c.sqlite3_column_type(stmt, index);
            const value: Column = switch (col_type) {
                c.SQLITE_INTEGER => .{ .integer = @intCast(c.sqlite3_column_int64(stmt, index)) },
                c.SQLITE_FLOAT => .{ .double = @floatCast(c.sqlite3_column_double(stmt, index)) },
                c.SQLITE_TEXT => .{
                    .text = try allocator.dupe(u8, std.mem.span(c.sqlite3_column_text(stmt, index))),
                },
                c.SQLITE_NULL => .none,
                else => unreachable,
            };

            try list.append(allocator, value);
        }

        return try list.toOwnedSlice(allocator);
    }

    const Row = struct {
        arena: std.heap.ArenaAllocator,
        columns: []const Column,

        pub fn deinit(self: Row) void {
            self.arena.deinit();
        }
    };

    pub fn fetch_row(
        self: Sqlite,
        parent_allocator: std.mem.Allocator,
        comptime sql: []const u8,
        params: anytype,
    ) !Row {
        var arena = std.heap.ArenaAllocator.init(parent_allocator);
        errdefer arena.deinit();
        const allocator = arena.allocator();

        var stmt: ?*c.sqlite3_stmt = null;

        const rc = c.sqlite3_prepare_v2(self.db, sql.ptr, @intCast(sql.len), &stmt, null);
        if (rc != c.SQLITE_OK) {
            log.err("sqlite3 prepare failed: {s}", .{c.sqlite3_errmsg(self.db)});
            return error.FailedPrepare;
        }
        defer _ = c.sqlite3_finalize(stmt);

        try bind_params(stmt.?, params);

        switch (c.sqlite3_step(stmt)) {
            c.SQLITE_ROW => {
                const columns = try parse_row(allocator, stmt.?);
                return .{ .arena = arena, .columns = columns };
            },
            c.SQLITE_DONE => return error.NotFound,
            else => unreachable,
        }
    }

    pub fn fetch_rows(
        self: Sqlite,
        parent_allocator: std.mem.Allocator,
        comptime sql: []const u8,
        params: anytype,
    ) ![]const Row {
        var stmt: ?*c.sqlite3_stmt = null;

        const rc = c.sqlite3_prepare_v2(self.db, sql.ptr, @intCast(sql.len), &stmt, null);
        if (rc != c.SQLITE_OK) {
            log.err("sqlite3 prepare failed: {s}", .{c.sqlite3_errmsg(self.db)});
            return error.FailedPrepare;
        }
        defer _ = c.sqlite3_finalize(stmt);

        try bind_params(stmt.?, params);

        var list = try std.ArrayListUnmanaged(Row).initCapacity(parent_allocator, 0);
        errdefer for (list.items) |row| row.deinit();
        defer list.deinit(parent_allocator);

        while (true) {
            switch (c.sqlite3_step(stmt)) {
                c.SQLITE_ROW => {
                    var arena = std.heap.ArenaAllocator.init(parent_allocator);
                    errdefer arena.deinit();

                    const columns = try parse_row(arena.allocator(), stmt.?);
                    const row = .{ .arena = arena, .columns = columns };

                    try list.append(parent_allocator, row);
                },
                c.SQLITE_DONE => return try list.toOwnedSlice(parent_allocator),
                else => unreachable,
            }
        }
    }

    // TODO: fetch_iterator
};

const testing = @import("std").testing;

test "Sqlite: General Flow" {
    const User = struct {
        name: []const u8,
        age: i32,
        weight: f32 = 10.0,
    };

    const connection = try Sqlite.open(":memory:");
    defer connection.close();

    try connection.execute(
        \\create table if not exists users (
        \\id integer primary key,
        \\name text not null,
        \\age integer
        \\)
    , .{});

    try connection.execute(
        \\insert into users (name, age) values (?, ?)
    , .{ "Alice", 25 });

    try connection.execute(
        \\insert into users (name, age) values (?, ?)
    , .{ "Jane", 99 });

    try connection.execute(
        \\insert into users (name, age) values (?, ?)
    , .{ "Girl", 7 });

    const john = try connection.fetch_optional(testing.allocator, User,
        \\select name, age from users
        \\where name = ?
    , .{"John"});

    try testing.expectEqual(null, john);

    const all_users = try connection.fetch_all(testing.allocator, User,
        \\select name, age from users
    , .{});

    defer testing.allocator.free(all_users);
    defer for (all_users) |user| testing.allocator.free(user.name);

    try testing.expectEqual(all_users.len, 3);

    for (all_users) |user| {
        try testing.expectEqual(10.0, user.weight);
    }

    const row = try connection.fetch_row(testing.allocator, "select age from users", .{});
    defer row.deinit();

    try testing.expectEqual(1, row.columns.len);
    switch (row.columns[0]) {
        .integer => {},
        else => std.debug.panic("got incorrect type: {}", .{row.columns[0]}),
    }

    const rows = try connection.fetch_rows(testing.allocator, "select name from users", .{});
    defer testing.allocator.free(rows);
    defer for (rows) |r| r.deinit();

    for (rows) |r| for (r.columns) |col| switch (col) {
        .text => {},
        else => std.debug.panic("got incorrect type: {}", .{col}),
    };
}
