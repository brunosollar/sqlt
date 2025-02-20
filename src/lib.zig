const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.sqlt);

pub const Sqlite = @import("sqlite/lib.zig").Sqlite;
pub const Postgres = @import("postgres/lib.zig").Postgres;

/// Internally exposed Tardy.
pub const tardy = @import("tardy");

pub fn migrate(db: anytype) !void {
    const db_type = @TypeOf(db);

    // TODO: create a table in the DB that tracks which migrations have already run...
    switch (comptime db_type) {
        Sqlite => log.debug("migrating on sqlite...", .{}),
        *Postgres => log.debug("migrating on postgres...", .{}),
        else => @compileError("Unsupported DB Type: " ++ @typeName(db_type)),
    }

    const migrations = @import("sqlt_migrations");
    const names_optional: ?[]const []const u8 = migrations.names;
    const contents_optional: ?[]const []const u8 = migrations.contents;

    if (comptime names_optional == null and contents_optional == null)
        @compileError("No migrations found! Try setting the 'sqlt_dir' flag in your build to point at the sqlt folder in your source.");

    const names = names_optional.?;
    const contents = contents_optional.?;
    assert(names.len == contents.len);
    inline for (names, contents) |name, content| {
        log.info("applying migration: {s}", .{name});
        try db.execute(content, .{});
    }
}
