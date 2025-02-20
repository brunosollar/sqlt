const std = @import("std");

const Message = @import("../src/postgres/message.zig").Message;

pub const Database = union(enum) {
    sqlite: []const u8,
    postgres: struct {
        user: []const u8 = "postgres",
        pass: ?[]const u8 = null,
        host: []const u8 = "localhost",
        port: u16 = 5432,
        database: []const u8 = "postgres",
    },

    pub fn parse_url(url: []const u8) !Database {
        if (std.mem.startsWith(u8, url, "sqlite:")) {
            // skip 'sqlite://'
            return .{ .sqlite = url[9..] };
        } else if (std.mem.startsWith(u8, url, "postgres:")) {
            var remaining = url[11..];
            var config: Database = .{ .postgres = .{} };

            if (std.mem.indexOfScalar(u8, remaining, '@')) |at_idx| {
                const auth = remaining[0..at_idx];
                if (std.mem.indexOfScalar(u8, auth, ':')) |colon_idx| {
                    config.postgres.user = auth[0..colon_idx];
                    config.postgres.pass = auth[colon_idx + 1 ..];
                } else config.postgres.user = auth;

                remaining = remaining[at_idx + 1 ..];
            }

            // remaining is now after the @ if there was one.
            if (std.mem.indexOfScalar(u8, remaining, '/')) |slash_idx| {
                const host_port = remaining[0..slash_idx];
                if (std.mem.indexOfScalar(u8, host_port, ':')) |colon_idx| {
                    config.postgres.host = host_port[0..colon_idx];
                    config.postgres.port = try std.fmt.parseUnsigned(u16, host_port[colon_idx + 1 ..], 10);
                } else config.postgres.host = host_port;

                remaining = remaining[slash_idx + 1 ..];
            } else return error.InvalidDatabaseUrl;

            if (std.mem.indexOfScalar(u8, remaining, '?')) |q_idx| {
                config.postgres.database = remaining[0..q_idx];
                remaining = remaining[q_idx + 1 ..];
            } else {
                config.postgres.database = remaining;
                remaining = remaining[remaining.len..];
            }

            if (remaining.len > 0) {
                var queries = std.mem.splitScalar(u8, remaining, '&');
                while (queries.next()) |kv| {
                    const eql_idx = std.mem.indexOfScalar(u8, kv, '=').?;
                    const key = kv[0..eql_idx];
                    const value = kv[eql_idx + 1 ..];

                    if (std.mem.eql(u8, key, "user"))
                        config.postgres.user = value
                    else if (std.mem.eql(u8, key, "password"))
                        config.postgres.pass = value;
                }
            }

            return config;
        } else {
            @panic("Unsupported DB");
        }
    }
};

const testing = std.testing;

test "Parse Sqlite URLs" {
    const sqlite_urls: []const []const u8 = &.{
        ":memory:",
        "data.db",
        "./test.db",
        "/tmp/sqlt.db",
        "hi/this/is/a/database.db",
    };

    for (sqlite_urls) |url| {
        const fmt = try std.fmt.allocPrint(testing.allocator, "sqlite://{s}", .{url});
        defer testing.allocator.free(fmt);
        const db = try Database.parse_url(fmt);
        try testing.expectEqualSlices(u8, db.sqlite, url);
    }
}

test "Parse Postgres Basic URL" {
    const url = "postgres://muki:password@localhost:9999/users";
    const db = try Database.parse_url(url);

    const pg = db.postgres;
    try testing.expectEqualSlices(u8, pg.user, "muki");
    try testing.expectEqualSlices(u8, pg.pass.?, "password");
    try testing.expectEqualSlices(u8, pg.host, "localhost");
    try testing.expectEqual(pg.port, 9999);
    try testing.expectEqualSlices(u8, pg.database, "users");
}

test "Parse Postgres URL with Params" {
    const url = "postgres://localhost:9999/users?user=muki&password=password";
    const db = try Database.parse_url(url);

    const pg = db.postgres;
    try testing.expectEqualSlices(u8, pg.user, "muki");
    try testing.expectEqualSlices(u8, pg.pass.?, "password");
    try testing.expectEqualSlices(u8, pg.host, "localhost");
    try testing.expectEqual(pg.port, 9999);
    try testing.expectEqualSlices(u8, pg.database, "users");
}
