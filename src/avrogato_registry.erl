%%% @doc
%%% This module offers a registry cache mechanism based on ETS tables.
%%% It uses two tables to do things:
%%%
%%% 1. a "schemas" table, which is an `ordered_set' that maps from
%%%    `{SchemaName, Version}' to the confluent ID
%%%
%%% 2. an "id" table, which maps a confluent schema id to other
%%%    metadata such as encoding and decoding functions, the schema,
%%%    and so on.
%%%
%%% The functions in this module then wrap both of these indices and
%%% the stateful management of the cache to hide most of the details.
%%%
%%% The functions in this module should NOT be called by users, unless
%%% you are looking for specifics of schema manipulation. In normal
%%% circumstances, everything you need should be in the `avrogato' module.
%%%
%%% Look at `dump_schemas/3' and `load_schemas/1' for operations-specific
%%% values to take snapshots of the registry and reload them.
%%% @end
-module(avrogato_registry).
-behaviour(gen_server).
-include("avrogato.hrl").
-include_lib("erlavro/include/erlavro.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/2, lookup_id/1, lookup_schema/1, lookup_schema/2,
         find_schema_id/2,
         sync/0, sync_id/1, sync_schema/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([dump_schemas/3, load_schemas/1]).

-define(SCHEMA_PREFIX, <<"avrogato">>). % used for erlavro decoding shenanigans

%% Base types used in creating the index
-type prefetch() :: all | [avrogato:schema_name()].
-type registry_url() :: string().

-type handler() :: {encoder_fun(), Confluent::header(), OCF::header()}.
-type encoder_fun() :: avro:encode_fun().
-type decoder() :: fun((binary()) -> term()).
-type header() :: term().
-type schema() :: erlavro:avro_type().
-export_type([prefetch/0, registry_url/0,
              handler/0, decoder/0, encoder_fun/0, header/0, schema/0]).

%% Index types from the records in the avrogato.hrl header
-opaque schema_entry() :: #schema{}.
-opaque id_entry() :: #id{}.
-export_type([schema_entry/0, id_entry/0]).

-record(state, {
          url :: registry_url(),
          headers = [] :: [{string(), string()}],
          opts = [] :: [{atom(), term()}]
        }).


%%%%%%%%%%%
%%% API %%%
%%%%%%%%%%%

%% @doc Start the worker that maintains the registry in sync. Assumes that
%% two ETS tables were created by its parent (in `avrogato_sup'), so that
%% in case of a failure by the registry, the callers can still keep moving.
-spec start_link(registry_url() | binary(), prefetch()) -> term().
start_link(Registry, PreFetch) when is_binary(Registry) ->
    start_link(unicode:characters_to_list(Registry), PreFetch);
start_link(Registry, PreFetch) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Registry, PreFetch], []).

%% @doc look up a schema definition by using its Confluent ID. The version can
%% be `undefined' in the returned handler if the schema was not previously
%% looked up by name, since the confluent registry does not return any
%% version-specific information when fetching an ID, just the schema.
%%
%% This function is expected to be used for decoding purposes, and as such
%% does not return encoder information. For encoding, you should look up
%% schemas by name and version with `lookup_schema/2'.
-spec lookup_id(avrogato:id()) ->
    {avrogato:schema_name(), avrogato:version() | undefined, decoder()}.
lookup_id(Id) ->
    case ets:lookup(?ID_TAB, Id) of
        [#id{id=Id, schema_id={Name, Vsn}, decoder=Decoder}] ->
            {Name, Vsn, Decoder};
        [] ->
            case sync_id(Id) of
                ok ->
                    case ets:lookup(?ID_TAB, Id) of
                        [#id{id=Id, schema_id={Name, Vsn}, decoder=Decoder}] ->
                            {Name, Vsn, Decoder};
                        _ ->
                            error(bad_id)
                    end;
                _ ->
                    error(bad_id)
            end
    end.

%% @doc Look up a schema name for encoding. This form is somewhat expensive
%% because it does not require a schema version to be passed. As such, it ends
%% up doing a partial scan of the ETS table holding the registry cache to
%% automatically grab and return the handler for the latest version found of
%% the given registry.
%%
%% By default, it is not being used by any of the front-end code (in
%% `avrogato'), but can be used for debugging or testing purposes.
-spec lookup_schema(avrogato:schema_name()) -> handler().
lookup_schema(Schema) ->
    Spec = [{#schema{schema_id={Schema, '$1'}, id='$2', _='_'}, [], ['$2']}],
    case ets:select(?SCHEMA_TAB, Spec) of
        [] ->
            case sync_schema(Schema) of
                ok ->
                    case ets:select(?SCHEMA_TAB, Spec) of
                        [] ->
                            error(bad_schema);
                        List ->
                            Id = lists:last(List),
                            [#id{handler=Handler}] = ets:lookup(?ID_TAB, Id),
                            Handler
                    end;
                _ ->
                    error(bad_schema)
            end;
        List ->
            Id = lists:last(List),
            [#id{handler=Handler}] = ets:lookup(?ID_TAB, Id),
            Handler
    end.

%% @doc Look up a schema by name and version for encoding. Should be the
%% function of choice to use when encoding. The handler returned is a triple
%% of the form `{EncoderFun, ConfluentHeader, OCFHeader}'. The `EncoderFun'
%% takes a single argument (the payload) and returns a flat binary.
%%
%% The `ConfluentHeader' can then be prepended, or specific code within
%% the `erlavro' library can be used to apply the `OCFHeader' properly.
%% These values are pre-cached to avoid having to generate them on each
%% call.
-spec lookup_schema(avrogato:schema_name(), avrogato:version()) -> handler().
lookup_schema(Schema, Version) ->
    Id = find_schema_id(Schema, Version),
    [#id{handler=Handler}] = ets:lookup(?ID_TAB, Id),
    Handler.

%% @doc Find the Confluent ID that matches a given Schema at a specific
%% version.
-spec find_schema_id(avrogato:schema_name(), avrogato:version()) -> avrogato:id().
find_schema_id(Schema, Version) ->
    case ets:lookup(?SCHEMA_TAB, {Schema, Version}) of
        [] ->
            case sync_schema(Schema) of
                ok ->
                    case ets:lookup(?SCHEMA_TAB, {Schema, Version}) of
                        [] -> error(bad_schema);
                        [#schema{id=Id}] -> Id
                    end;
                _ ->
                    error(bad_schema)
            end;
        [#schema{id=Id}] ->
            Id
    end.

%% @doc Fetch *ALL* the known versions for all the schemas in the registry.
%% This function can take a long time to run and shouldn't be run outside of
%% when creating specific snapshot dumps, or when you specifically have a
%% small schema registry whose size you know.
%%
%% Schema versions that were already seen will not be refreshed.
-spec sync() -> ok.
sync() ->
    gen_server:call(?MODULE, sync, infinity).

%% @doc Fetch the definition of a specific schema according to its
%% Confluent ID.
%%
%% Bad schema versions will be omitted, although a log warning
%% will be emitted.
%% Schema versions that were already seen succesfully will not
%% be refreshed.
-spec sync_id(avrogato:id()) -> ok | {error, term()}.
sync_id(Id) ->
    gen_server:call(?MODULE, {sync, {id, Id}}, infinity).

%% @doc Fetch all version definitions of a specific schema, and
%% its matching confluent IDs.
%%
%% Bad schema versions will be omitted, although a log warning
%% will be emitted.
%% Schema versions that were already seen succesfully will not
%% be refreshed.
-spec sync_schema(avrogato:schema_name()) -> ok | {error, term()}.
sync_schema(Name) ->
    gen_server:call(?MODULE, {sync, {schema, Name}}, infinity).

%%%%%%%%%%%%%%%%%
%%% CALLBACKS %%%
%%%%%%%%%%%%%%%%%

%% @private
init([Registry, PreFetch]) ->
    {Hdrs, Opts} = avrogato_httpc:opts(Registry),
    State = #state{url=Registry, headers=Hdrs, opts=Opts},
    _ = sync_schemas(State, PreFetch), % best effort
    {ok, State}.

%% @private
handle_call(sync, _From, S=#state{}) ->
    {reply, sync_schemas(S, all), S};
handle_call({sync, {schema, Name}}, _From, S=#state{}) ->
    {reply, sync_schemas(S, [Name]), S};
handle_call({sync, {id, Id}}, _From, S=#state{}) ->
    try
        %% Check if a concurrent request has already fetched the
        %% value and if so, shortcircuit it.
        case ets:lookup(?ID_TAB, Id) of
            [] -> noskip;
            [_|_] -> throw(skip)
        end,
        %% ID-based fetching cannot find the version for a schema,
        %% and so we can only populate half the index here.
        New = get_id(S, Id),
        IdEntries = prepare_ids([New]),
        ets:insert(?ID_TAB, IdEntries),
        {reply, ok, S}
    catch
        throw:skip ->
            {reply, ok, S};
        error:R ->
            {reply, {error, R}, S}
    end.

%% @private
handle_cast(_Cast, State) ->
    {noreply, State}.

%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_, _) ->
    ok.

%%%%%%%%%%%%%%%
%%% PRIVATE %%%
%%%%%%%%%%%%%%%

%% @private synchronize all known schemas. Just report
%% whatever error happened since erlavro is pretty aggressive about
%% raising exceptions.
-spec sync_schemas(#state{}, prefetch()) -> ok.
sync_schemas(Registry, all) ->
    try list_schemas(Registry) of
        Schemas -> sync_schemas(Registry, Schemas)
    catch
        error:R ->
            {error, R}
    end;
sync_schemas(Registry, Schemas) ->
    try
        SchemaVsns = list_schema_versions(Registry, Schemas),
        ToFetch = prune_schema_versions(SchemaVsns),
        New = get_schemas(Registry, ToFetch),
        {SchemaEntries, IdEntries} = prepare_schemas(New),
        ets:insert(?SCHEMA_TAB, SchemaEntries),
        ets:insert(?ID_TAB, IdEntries),
        ok
    catch
        error:R ->
            {error, R}
    end.

%% @private list all the possible schemas (subjects) in the registry
-spec list_schemas(#state{}) -> [string()].
list_schemas(S=#state{url = Url}) ->
    URI = Url ++ "/subjects",
    Body = request(S#state{url = URI}),
    [unicode:characters_to_list(Entry) || Entry <- jsone:decode(Body)].

%% @private for all schemas, fetch all versions the registry knows about
-spec list_schema_versions(#state{}, [string()]) ->
    [{string(), [integer(), ...]}].
list_schema_versions(Registry, Schemas) ->
    [list_schema_versions_(Registry, Schema) || Schema <- Schemas].

%% @private for a given schema (subject), find all the versions the registry
%% knows about.
-spec list_schema_versions_(#state{}, string()) ->
    {string(), [integer(), ...]}.
list_schema_versions_(S=#state{url = Url}, Schema) ->
    URI = Url ++ "/subjects/" ++ unicode:characters_to_list(Schema) ++ "/versions",
    Body = request(S#state{url = URI}),
    {Schema, jsone:decode(Body)}.

%% @private drop the schema versions that we already know about from the list,
%% allowing us to save a bunch of queries on definitions already cached.
-spec prune_schema_versions(SchemaVsns) -> SchemaVsns when
      SchemaVsns :: [{string(), [integer(), ...]}].
prune_schema_versions(Schemas) ->
    lists:foldl(fun({Schema, Vsns}, Acc) ->
        Missing = [V || V <- Vsns, ets:lookup(?SCHEMA_TAB, {Schema, V}) =:= []],
        case Missing of
            [] -> Acc;
            _ -> [{Schema, Missing} | Acc]
        end
    end, [], Schemas).

%% @private Fetch the schema definitions for a given {schema, version} pair.
-spec get_schemas(#state{}, [{Name, [Vsn, ...]}]) -> [{{Name, Vsn}, Schema}] when
      Name :: string(),
      Vsn :: avrogato:version(),
      Schema :: {avrogato:id(), binary()}.
get_schemas(_, []) ->
    [];
get_schemas(Reg, [{Name, Vsns}|T]) ->
    [{{Name, Vsn}, get_schema_version(Reg, Name, Vsn)} || Vsn <- Vsns]
    ++ get_schemas(Reg, T).

%% @private Fetch a schema definition at a specific version.
-spec get_schema_version(#state{}, string(), avrogato:version()) ->
    {avrogato:id(), binary()}.
get_schema_version(S=#state{url = Url}, Name, Vsn) ->
    URI = Url ++ "/subjects/" ++ unicode:characters_to_list(Name)
        ++ "/versions/" ++ integer_to_list(Vsn),
    Body = request(S#state{url = URI}),
    #{<<"id">> := Id, <<"schema">> := JSON} = jsone:decode(Body),
    {Id, JSON}.

%% @private fetch a schema definition according to a confluent ID
-spec get_id(#state{}, avrogato:id()) -> {avrogato:id(), binary()}.
get_id(S=#state{url = Url}, Id) ->
    URI = Url ++ "/schemas/ids/" ++ integer_to_list(Id),
    Body = request(S#state{url = URI}),
    #{<<"schema">> := JSON} = jsone:decode(Body),
    {Id, JSON}.

%% @private Take a list of all downloaded schemas, and prepare them for the format
%% of our in-memory cache index
-spec prepare_schemas([{{string(), avrogato:version()}, {avrogato:id(), binary()}}]) ->
    {[schema_entry()], [id_entry()]}.
prepare_schemas(List) ->
    prepare_schemas(List, {[], []}).

prepare_schemas([], Acc) ->
    Acc;
prepare_schemas([H|T], {Schemas, Ids}) ->
    case prepare_schema(H) of
        {ok, {Schema, Id}} ->
            prepare_schemas(T, {[Schema|Schemas], [Id|Ids]});
        {error, Reason} ->
            %% Log and skip bad results; the schema registry may
            %% contain invalid entries in older versions of a schema.
            {Name, _} = H,
            ?LOG_WARNING(#{
               in => "avrogato_schema", what => {schema, Name},
               result => error, reason => Reason
            }),
            prepare_schemas(T, {Schemas, Ids})
    end.

%% @private Map a given schema definition to the proper in-memory index format.
%% Most of the heavy lifting is deffered to `prepare_id/1', but this
%% function also adds in the "schema" part of the index, and augments the
%% returned values with a proper schema version.
-spec prepare_schema({{string(), avrogato:version()}, {avrogato:id(), binary()}}) ->
        {ok, {schema_entry(), id_entry()}} | {error, term()}.
prepare_schema({{Name, Vsn}, {Id, JSON}}) ->
    BinName = unicode:characters_to_binary(Name),
    case prepare_id({Id, JSON}) of
        {ok, IdEntry = #id{id=Id, schema_id={BinName, _}}} ->
            %% the version is `undefined' in prepare_id/1, add it in when
            %% adding the schema tuple.
            {ok, {#schema{schema_id={BinName, Vsn}, id=Id},
                  IdEntry#id{schema_id={BinName, Vsn}}}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Take a list of all downloaded schemas, and prepare them for the format
%% of our in-memory cache index
-spec prepare_ids([{avrogato:id(), binary()}]) -> [id_entry()].
prepare_ids([]) ->
    [];
prepare_ids([H|T]) ->
    case prepare_id(H) of
        {ok, Ready} ->
            [Ready | prepare_ids(T)];
        {error, Reason} ->
            %% Log and skip bad results; the schema registry may
            %% contain invalid entries in older versions of a schema.
            {Id, _JSON} = H,
            ?LOG_WARNING(#{
               in => "avrogato_schema", what => {id, Id},
               result => error, reason => Reason
            }),
            prepare_ids(T)
    end.

%% @private This is where the heaviest lifting is done; the schema is
%% decoded, the record name extracted, and all the early work regarding
%% caching of operations to speed them up is done here: headers are
%% pre-computed for both supported formats, encoder and decoder functions
%% are primed, and the index format for ID-based searched is prepared.
-spec prepare_id({avrogato:id(), binary()}) -> {ok, id_entry()} | {error, term()}.
prepare_id({Id, JSON}) ->
    NewStore = avro_schema_store:new([{dict, true}]),
    try
        Schema = avro:decode_schema(JSON),
        #avro_record_type{fullname = BinName} = Schema,
        %% can't have the fullname be the internal name when
        %% calling the schema store, so we prefix with ?SCHEMA_PREFIX
        SchemaName = <<(?SCHEMA_PREFIX)/binary, ".", BinName/binary>>,
        Store = avro_schema_store:add_type(SchemaName, Schema, NewStore),
        Encoder = avro:make_encoder(Store, [{encoding, avro_binary}]),
        ConfluentHeader = <<?CONFLUENT_VSN:8, Id:32/unsigned>>,
        OCFHeader = avro_ocf:make_header(Schema),
        Handler = {Encoder, ConfluentHeader, OCFHeader},
        AvroDecoder = avro:make_decoder(
            fun(?SCHEMA_PREFIX) -> Schema end,
            [{encoding, avro_binary}]
        ),
        Decoder = fun(Bin) -> AvroDecoder(?SCHEMA_PREFIX, Bin) end,
        {ok, #id{id=Id, schema_id={BinName, undefined}, schema=Schema,
                 handler=Handler, decoder=Decoder}}
    catch
        Type:Reason ->
            {error, {Type, Reason}}
    end.

%% @private Send actual HTTP requests; return the body only if the code is good.
-spec request(#state{}) -> binary().
request(#state{url = URI, headers = Headers, opts = Opts}) ->
    avrogato_httpc:request(URI, Headers, Opts).


%%%%%%%%%%%
%%% OPS %%%
%%%%%%%%%%%

%% @doc function to dump a given list of schemas to a file
%% that can be re-hydrated as a loadable registry; useful
%% to cope with offline cases, or to prep test content.
%% The URL must be a list.
-spec dump_schemas(registry_url(), file:filename(), prefetch()) -> ok.
dump_schemas(Registry, Path, all) ->
    {Hdrs, Opts} = avrogato_httpc:opts(Registry),
    State = #state{url=Registry, headers=Hdrs, opts=Opts},
    dump_schemas(Registry, Path, list_schemas(State));
dump_schemas(Registry, Path, Schemas) ->
    {Hdrs, Opts} = avrogato_httpc:opts(Registry),
    State = #state{url=Registry, headers=Hdrs, opts=Opts},
    ToFetch = list_schema_versions(State, Schemas),
    Fetched = get_schemas(State, ToFetch),
    file:write_file(Path, term_to_binary(Fetched, [compressed])).

%% @doc load a set of dumped schemas back in memory
-spec load_schemas(file:filename()) -> ok.
load_schemas(Path) ->
    {ok, Bin} = file:read_file(Path),
    Fetched = binary_to_term(Bin),
    {SchemaEntries, IdEntries} = prepare_schemas(Fetched),
    ets:insert(?SCHEMA_TAB, SchemaEntries),
    ets:insert(?ID_TAB, IdEntries),
    ok.
