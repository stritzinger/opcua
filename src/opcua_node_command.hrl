
%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type timestamp_type() :: source | server | both | neither | invalid.
-type max_age() :: cached | newest | pos_integer().
-type read_options() :: #{
    max_age => max_age(),
    timestamp_type => timestamp_type()
}.
-type opcua_range() :: Index :: non_neg_integer()
                     | {Min :: non_neg_integer(), Max :: non_neg_integer()}.

-record(read_attribute, {
    attr :: atom() | pos_integer(),
    range :: undefined | [opcua_range()],
    opts  = #{} :: read_options()
}).
