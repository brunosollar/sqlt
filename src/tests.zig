const std = @import("std");
const testing = std.testing;

test "sqlt unit tests" {
    // API
    testing.refAllDecls(@import("sqlite/lib.zig"));
}
