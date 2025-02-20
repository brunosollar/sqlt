const std = @import("std");

const Database = @import("parse.zig").Database;

fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next().?;

    const sqlt_dir_path = args.next().?;
    var sqlt_dir = try std.fs.cwd().openDir(sqlt_dir_path, .{ .iterate = true });
    defer sqlt_dir.close();

    const db_url = args.next().?;
    const db = try Database.parse_url(db_url);

    switch (db) {
        .sqlite => |path| {
            _ = path;
        },
        .postgres => |config| {
            _ = config;
        },
    }
}
