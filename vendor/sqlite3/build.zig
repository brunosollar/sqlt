const std = @import("std");
const builtin = @import("builtin");
const Build = std.Build;

pub fn build(b: *Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const upstream = b.dependency("sqlite3", .{
        .target = target,
        .optimize = optimize,
    });

    const sqlite3 = b.addStaticLibrary(.{
        .name = "sqlite3",
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    sqlite3.addCSourceFiles(.{
        .root = upstream.path(""),
        .files = &.{"sqlite3.c"},
    });

    sqlite3.addIncludePath(upstream.path(""));

    sqlite3.installHeader(upstream.path("sqlite3.h"), "sqlite3.h");

    b.installArtifact(sqlite3);
}
