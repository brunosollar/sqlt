const std = @import("std");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const sqlt_dir = b.option([]const u8, "sqlt_dir", "Location of your sqlt folder") orelse "./sqlt";

    const sqlt = b.addModule("sqlt", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    const migrations = try create_migrations_module(b, sqlt_dir);
    sqlt.addImport("sqlt_migrations", migrations);

    const sqlite3 = b.dependency("sqlite3", .{
        .target = target,
        .optimize = optimize,
    }).artifact("sqlite3");

    sqlt.linkLibrary(sqlite3);

    const tardy = b.dependency("tardy", .{
        .target = target,
        .optimize = optimize,
    }).module("tardy");

    sqlt.addImport("tardy", tardy);

    add_example(b, "sqlite", false, target, optimize, tardy, sqlt);
    add_example(b, "postgres", false, target, optimize, tardy, sqlt);

    const tests = b.addTest(.{
        .name = "tests",
        .root_source_file = b.path("./src/tests.zig"),
        .link_libc = true,
    });

    tests.linkLibrary(sqlite3);
    tests.root_module.addImport("tardy", tardy);

    const run_test = b.addRunArtifact(tests);
    run_test.step.dependOn(&tests.step);

    const test_step = b.step("test", "Run general unit tests");
    test_step.dependOn(&run_test.step);
}

fn add_example(
    b: *std.Build,
    name: []const u8,
    link_libc: bool,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Mode,
    tardy: *std.Build.Module,
    sqlt: *std.Build.Module,
) void {
    const example_root = b.fmt("./examples/{s}", .{name});

    const example = b.addExecutable(.{
        .name = name,
        .root_source_file = b.path(b.fmt("{s}/main.zig", .{example_root})),
        .target = target,
        .optimize = optimize,
        .strip = false,
    });

    if (link_libc) example.linkLibC();

    example.root_module.addImport("tardy", tardy);
    example.root_module.addImport("sqlt", sqlt);

    const install_artifact = b.addInstallArtifact(example, .{});
    b.getInstallStep().dependOn(&install_artifact.step);

    const build_step = b.step(b.fmt("{s}", .{name}), b.fmt("Build sqlt example ({s})", .{name}));
    build_step.dependOn(&install_artifact.step);

    const run_artifact = b.addRunArtifact(example);
    run_artifact.step.dependOn(&install_artifact.step);

    const run_step = b.step(b.fmt("run_{s}", .{name}), b.fmt("Run sqlt example ({s})", .{name}));
    run_step.dependOn(&install_artifact.step);
    run_step.dependOn(&run_artifact.step);
}

fn create_migrations_module(b: *std.Build, sqlt_dir: []const u8) !*std.Build.Module {
    const Migration = struct {
        name: []const u8,
        contents: []const u8,
    };

    const MigrationQueue = std.PriorityQueue(Migration, void, struct {
        fn extract_order_id(name: []const u8) usize {
            const dash_idx = std.mem.indexOfScalar(u8, name, '-') orelse
                @panic("Migration files must be in the [num]-[name].sql format!");
            return std.fmt.parseUnsigned(usize, name[0..dash_idx], 10) catch
                @panic("Migration files must be in the [num]-[name].sql format!");
        }

        fn compare_fn(_: void, first: Migration, second: Migration) std.math.Order {
            return std.math.order(extract_order_id(first.name), extract_order_id(second.name));
        }
    }.compare_fn);

    var queue = MigrationQueue.init(b.allocator, {});
    defer queue.deinit();

    const path = b.fmt("{s}/migrations", .{sqlt_dir});

    var dir = std.fs.cwd().openDir(path, .{ .iterate = true }) catch {
        const options = b.addOptions();
        options.addOption(?[]const []const u8, "names", null);
        options.addOption(?[]const []const u8, "contents", null);
        return options.createModule();
    };

    defer dir.close();
    var it = dir.iterate();

    while (try it.next()) |entry| if (std.mem.endsWith(u8, entry.name, ".sql")) {
        const contents = try dir.readFileAlloc(b.allocator, entry.name, std.math.maxInt(usize));
        const migration: Migration = .{ .name = b.dupe(entry.name), .contents = contents };
        try queue.add(migration);
    };

    var list = std.ArrayList(Migration).init(b.allocator);
    while (queue.removeOrNull()) |item| try list.append(item);

    const names = try b.allocator.alloc([]const u8, list.items.len);
    const contents = try b.allocator.alloc([]const u8, list.items.len);
    for (list.items, 0..) |item, i| {
        names[i] = item.name;
        contents[i] = item.contents;
    }

    const options = b.addOptions();
    options.addOption(?[]const []const u8, "names", names);
    options.addOption(?[]const []const u8, "contents", contents);
    return options.createModule();
}
