const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.sqlt);

pub const Sqlite = @import("sqlite/lib.zig").Sqlite;
pub const Postgres = @import("postgres/lib.zig").Postgres;

/// Internally exposed Tardy.
pub const tardy = @import("tardy");

const MigrationSql = struct {
    create_table: []const u8,
    select_with_hash: []const u8,
    insert_into: []const u8,
};

pub fn migrate(allocator: std.mem.Allocator, db: anytype) !void {
    const MigrationEntry = struct { hash: i64, name: []const u8 };

    const migrations = @import("sqlt_migrations");
    const names_optional: ?[]const []const u8 = migrations.names;
    const contents_optional: ?[]const []const u8 = migrations.contents;

    if (comptime names_optional == null and contents_optional == null)
        @compileError("No migrations found! Try setting the 'sqlt_dir' flag in your build to point at the sqlt folder in your source.");

    const names = names_optional.?;
    const contents = contents_optional.?;
    assert(names.len == contents.len);

    const sql_pack: MigrationSql = comptime switch (@TypeOf(db)) {
        Sqlite => .{
            .create_table =
            \\create table if not exists _sqlt_migrations (
            \\ hash integer primary key,
            \\ name text not null
            \\)
            ,
            .select_with_hash =
            \\ select name, hash from _sqlt_migrations
            \\ where hash = ?
            ,
            .insert_into =
            \\ insert into _sqlt_migrations (hash, name)
            \\ values (?, ?)
            ,
        },
        *Postgres => .{
            .create_table =
            \\create table if not exists _sqlt_migrations (
            \\ hash bigint primary key,
            \\ name text not null
            \\)
            ,
            .select_with_hash =
            \\ select name, hash from _sqlt_migrations
            \\ where hash = $1
            ,
            .insert_into =
            \\ insert into _sqlt_migrations (hash, name)
            \\ values ($1, $2)
            ,
        },
        else => unreachable,
    };

    try db.execute(sql_pack.create_table, .{});

    inline for (names, contents) |n, c| {
        const hash: i64 = @bitCast(comptime std.hash.Wyhash.hash(0, c));

        const found = try db.fetch_optional(
            allocator,
            MigrationEntry,
            sql_pack.select_with_hash,
            .{hash},
        );

        if (found) |f| {
            defer allocator.free(f.name);
            if (!std.mem.eql(u8, f.name, n)) return error.MismatchedMigrations;
        } else {
            log.info("applying migration: {s}", .{n});
            try db.execute(c, .{});
            try db.execute(sql_pack.insert_into, .{ hash, n });
        }
    }
}
