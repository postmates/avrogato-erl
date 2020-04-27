-define(CONFLUENT_VSN, 0).

%% Registry index-related definitions
-define(SCHEMA_TAB, avrogato_registry_schemas).
-define(ID_TAB, avrogato_registry_ids).

%% the atoms used in the types below are for match specs
%% in ets:select/2 to typecheck, but shouldn't be used otherwise...
-record(schema, {schema_id :: {avrogato:schema_name(), avrogato:version() | atom()},
                 id :: avrogato:id() | atom()}).

-record(id, {id :: avrogato:id(),
             schema_id :: {avrogato:schema_name(),
                           avrogato:version() | undefined},
             schema :: avrogato_registry:schema(),
             handler :: avrogato_registry:handler(),
             decoder :: avrogato_registry:decoder()}).
