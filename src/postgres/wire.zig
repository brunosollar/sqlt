// The Postgres Wire Protocol is like a state machine.
// This will represent the central engine.
pub const Wire = struct {
    const Stage = enum {
        startup,
        simple_query,
        extended_query,
    };
};

// We connect to the Postgres Backend
// We send a startup message
// PG responds asking for auth
// We respond with the auth
// PG either replies with ErrorResponse if rejected or AuthenticationOk if good.
// We then wait for PG to send another message
// PG then sends either:
//  BackendKeyData
//  ParameterStatus
//  ReadyForQuery -> this is our standard ready to take in a SQL query message
//  ErrorResponse
//  NoticeResponse
