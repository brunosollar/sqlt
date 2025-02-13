const Sqlite = @import("./lib.zig").Sqlite;

pub const SqlitePool = struct {
    // basically should just be an AtomicPool.
    // you either borrow (acquire) or you wait for one to be available :)
    // then you release it back, waking up whichever is waiting (in an AtomicQueue so FIFO to minimize tail latency)
    // ^ this can probably all be in the AtomicPool?
    //
    // then you would just need to open all of the connections and close them all on deinit
};
