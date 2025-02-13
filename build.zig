const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const sqlt = b.addModule("sqlt", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

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
    tardy_module: *std.Build.Module,
    sqlt_module: *std.Build.Module,
) void {
    const example = b.addExecutable(.{
        .name = name,
        .root_source_file = b.path(b.fmt("./examples/{s}/main.zig", .{name})),
        .target = target,
        .optimize = optimize,
        .strip = false,
    });

    if (link_libc) example.linkLibC();

    example.root_module.addImport("tardy", tardy_module);
    example.root_module.addImport("sqlt", sqlt_module);

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
