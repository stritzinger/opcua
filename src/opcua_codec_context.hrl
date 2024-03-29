%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(ctx, {
    mode :: opcua_codec_context:mode(),
    space :: opcua_space:state(),
    allow_partial :: boolean(),
    stack = [] :: [binary() | atom() | integer() | string()
                   | {binary()} | {atom()} | {integer()} | {string()}],
    issues = [] :: [term()] % Internal format is opaque
}).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Push key into the context stack
-define(PUSHK(CTX, KEY), opcua_codec_context:push((CTX), key, (KEY))).
% Push field into the context stack
-define(PUSHF(CTX, FIELD), opcua_codec_context:push((CTX), field, (FIELD))).
% Pop key from the context stack
-define(POPK(CTX, KEY), opcua_codec_context:pop((CTX), key, (KEY))).
% Pop field from the context stack
-define(POPF(CTX, FIELD), opcua_codec_context:pop((CTX), field, (FIELD))).
