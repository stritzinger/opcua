
%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(IS_BUILTIN_TYPE(T),
    (T =:= boolean);
    (T =:= byte);
    (T =:= byte);
    (T =:= uint16);
    (T =:= uint32);
    (T =:= uint64);
    (T =:= int16);
    (T =:= int32);
    (T =:= int64);
    (T =:= float);
    (T =:= double);
    (T =:= string);
    (T =:= date_time);
    (T =:= guid);
    (T =:= xml);
    (T =:= status_code);
    (T =:= byte_string);
    (T =:= node_id);
    (T =:= expanded_node_id);
    (T =:= diagnostic_info);
    (T =:= qualified_name);
    (T =:= localized_text);
    (T =:= extension_object);
    (T =:= variant);
    (T =:= data_value)
).
