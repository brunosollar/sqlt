const std = @import("std");

pub const Message = struct {
    const Header = extern struct {
        ident: u8,
        length: i32,
    };
};
