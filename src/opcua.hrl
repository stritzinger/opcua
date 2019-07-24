
%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(node_id, {
    ns = default :: default | non_neg_integer(),
    type = numeric :: numeric | string | guid | opaque,
    value :: non_neg_integer() | binary()
}).
