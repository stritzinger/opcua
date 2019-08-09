
%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type timestamp_type() :: source | server | both | neither | invalid.
-type max_age() :: cached | newest | pos_integer().
-type read_options() :: #{
    max_age => max_age(),
    timestamp_type => timestamp_type()
}.
-type browse_options() :: #{
    max_refs => non_neg_integer()
}.

-type opcua_range() :: Index :: non_neg_integer()
                     | {Min :: non_neg_integer(), Max :: non_neg_integer()}.

-record(read_command, {
    attr :: atom() | pos_integer(),
    range :: undefined | [opcua_range()],
    opts  = #{} :: read_options()
}).

-type read_result() :: #data_value{}.

-record(browse_command, {
    type :: undefined | node_id(),
    subtypes = true :: boolean(),
    direction = forward :: forward | inverse | both,
    opts = #{} :: browse_options()
}).

-type reference_description() :: #{
    node_id := node_id(),
    reference_type_id => node_id(),
    is_forward => boolean(),
    browse_name => #qualified_name{},
    display_name => #localized_text{},
    node_class => atom(),
    type_definition => #expanded_node_id{}
}.
-type browse_result() :: #{
    status => atom(),
    references => [reference_description()]
}.
