-module(opcua_test_util).

% API
-export([without_error_logger/1]).

%--- API -----------------------------------------------------------------------

without_error_logger(Fun) ->
    error_logger:tty(false),
    try
        Fun()
    after
        error_logger:tty(true)
    end.
