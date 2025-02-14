const std = @import("std");
const log = std.log.scoped(.@"sqlt/postgres/wire");
const assert = std.debug.assert;

const ZeroCopy = @import("tardy").ZeroCopy;
const Message = @import("message.zig").Message;

const ParsingInfo = struct {
    const State = enum {
        header,
        payload,
    };

    state: State = .header,
    kind: ?u8 = null,
    length: ?i32 = null,
    payload: ?[]const u8 = null,
};

pub const Wire = struct {
    allocator: std.mem.Allocator,
    send_buffer: std.ArrayListUnmanaged(u8),
    recv_zc_buffer: ZeroCopy(u8),
    bytes_processed: usize,
    parsing: ParsingInfo,

    pub fn init(allocator: std.mem.Allocator) !Wire {
        var send_buffer = try std.ArrayListUnmanaged(u8).initCapacity(allocator, 128);
        errdefer send_buffer.deinit(allocator);

        var recv_zc_buffer = try ZeroCopy(u8).init(allocator, 128);
        errdefer recv_zc_buffer.deinit();

        return .{
            .allocator = allocator,
            .send_buffer = send_buffer,
            .recv_zc_buffer = recv_zc_buffer,
            .bytes_processed = 0,
            .parsing = .{},
        };
    }

    pub fn deinit(self: *Wire) void {
        self.send_buffer.deinit(self.allocator);
        self.recv_zc_buffer.deinit();
    }

    pub fn next_recv(self: *Wire) ![]u8 {
        return try self.recv_zc_buffer.get_write_area(1024);
    }

    pub fn mark_recv(self: *Wire, length: usize) void {
        self.recv_zc_buffer.mark_written(length);
    }

    // This Message is only valid until the next *_recv call into Wire.
    pub fn process_recv(self: *Wire) !?Message.Backend {
        const bytes = self.recv_zc_buffer.subslice(.{ .start = self.bytes_processed });
        log.debug("bytes length={d}", .{bytes.len});

        while (true) {
            switch (self.parsing.state) {
                .header => {
                    if (bytes.len < 5) return null;
                    self.parsing.kind = bytes[0];
                    log.debug("got message kind={c}", .{bytes[0]});
                    self.parsing.length = std.mem.readInt(i32, bytes[1..5], .big);
                    log.debug("got message length={d}", .{self.parsing.length.?});
                    self.parsing.state = .payload;
                },
                .payload => {
                    const total_length: usize = @intCast(self.parsing.length.? + 1);
                    if (bytes.len >= total_length) {
                        const msg = try Message.Backend.parse(
                            self.allocator,
                            self.parsing.kind.?,
                            bytes[5..total_length],
                        );
                        self.bytes_processed += total_length;

                        // Clear if we have processed everything.
                        if (self.bytes_processed == self.recv_zc_buffer.len) {
                            self.recv_zc_buffer.clear_retaining_capacity();
                            self.bytes_processed = 0;
                        }

                        self.parsing = .{};
                        return msg;
                    } else return null;
                },
            }
        }
    }
};
