%%% @doc
%%% Main interface module for the application.
%%% All required interactions to encode and decode data should be possible
%%% to handle here.
%%%
%%% Interactions with the confluent Schema registry required by this
%%% application should be fully opaque and encapsulated.
%%%
%%% If you still need to do something with the registry, take a look at
%%% `avrogato_registry'.
%%% @end
-module(avrogato).
%-export([loaded_schemas/0, handler/1, encode/2, decode/1]).
-export([loaded_schemas/0,
         encode/2, decode/2, encode_ocf/2, decode_ocf/2, decode_any/1]).
-include("avrogato.hrl").
-include_lib("erlavro/include/erlavro.hrl").

-type schema_name() :: avrogato_schemas:fullname(). % namespace.typename
-type version() :: non_neg_integer().
-type id() :: non_neg_integer().
-type record() :: [{binary(), record() | term()}].

-export_type([schema_name/0, version/0, id/0, record/0]).

%% @doc Encode a given record key/value pair with the schema specification
%% `{Name, Version}', where `Name' is the full schema name
%% (`<<"namespace.record">>'), and `Version' is the schema registry's
%% number for the current record definition.
%%
%% The encoding is done using the Confluent format and header with a
%% unique global ID.
-spec encode({schema_name(), version()}, record()) -> iolist().
encode({Name, Vsn}, Record) ->
    {Encoder, ConfluentHdr, _} = avrogato_registry:lookup_schema(Name, Vsn),
    Bin = Encoder(Name, Record),
    [ConfluentHdr, Bin].

%% @doc Encode a given record key/value pair with the schema specification
%% `{Name, Version}', where `Name' is the full schema name
%% (`<<"namespace.record">>'), and `Version' is the schema registry's
%% number for the current record definition.
%%
%% The encoding is done using the OCF format, which inlines the schema
%% definition before the actual payload.
-spec encode_ocf({schema_name(), version()}, record()) -> iolist().
encode_ocf({Name, Vsn}, Record) ->
    {Encoder, _, OCFHeader} = avrogato_registry:lookup_schema(Name, Vsn),
    Bin = Encoder(Name, Record),
    avro_ocf:make_ocf(OCFHeader, [Bin]).

%% @doc Decode a given payload to a record key/value pair, while checking
%% against the expected schema specification `{Name, Version}'.
%%
%% The encoded data must have a header, either using a confluent global ID
%% or an OCF payload. The expected `{Name, Version}' do not impact the
%% decoding, only returning data from a schema which the calling code might
%% not expect.
-spec decode({schema_name(), version()}, iodata()) -> {ok, iolist()}
                                                    | {error, schema_mismatch}.
decode({Name, Vsn}, Bin) when is_binary(Bin) ->
    case Bin of
        <<?CONFLUENT_VSN:8, Id:32/unsigned, Payload/binary>> ->
            case avrogato_registry:lookup_id(Id) of
                {Name, undefined, Decoder} ->
                    try avrogato_registry:find_schema_id(Name, Vsn) of
                        Id -> {ok, Decoder(Payload)};
                        _ -> {error, schema_mismatch}
                    catch
                        error:bad_schema ->
                            {error, schema_mismatch}
                    end;
                {Name, Vsn, Decoder} ->
                    {ok, Decoder(Payload)};
                {_Name, _Vsn, _} ->
                    {error, schema_mismatch}
            end;
        _ ->
            decode_ocf({Name, Vsn}, Bin)
    end;
decode({Name, Vsn}, IoList) ->
    decode({Name, Vsn}, iolist_to_binary(IoList)).

%% @doc Specifically decode OCF data. The `{Name, Version}' tuple is
%% still required even if we can't check the version from the OCF data
%% but the name is still checked. The `Version' is still expected for
%% API consistency.
-spec decode_ocf({schema_name(), version()}, iodata()) ->
    {ok, iolist()} | {error, schema_mismatch}.
decode_ocf({Name, _Vsn}, Bin) when is_binary(Bin) ->
    case avro_ocf:decode_binary(Bin) of
        {_Header, #avro_record_type{fullname = Name}, [T]} ->
            {ok, T};
        _ ->
            {error, schema_mismatch}
    end;
decode_ocf({Name, Vsn}, IoList) ->
    decode_ocf({Name, Vsn}, iolist_to_binary(IoList)).

%% @doc Decode the incoming data without any care for its name or version,
%% and either using the confluent or the OCF format.
%% The returned value still contains the full name of the schema used, to
%% aid in handling the resulting record.
-spec decode_any(iodata()) -> {schema_name(), record()}.
decode_any(<<?CONFLUENT_VSN:8, Id:32/unsigned, Payload/binary>>) ->
    {Name, _, Decoder} = avrogato_registry:lookup_id(Id),
    {Name, Decoder(Payload)};
decode_any(Bin) when is_binary(Bin) ->
    {_Header, Record, [T]} = avro_ocf:decode_binary(Bin),
    #avro_record_type{fullname = Name} = Record,
    {Name, T};
decode_any(IoList) ->
    decode_any(iolist_to_binary(IoList)).

% @doc return a list of all the schemas found.
% Assumes the application is started before being called.
-spec loaded_schemas() -> [{schema_name(), version()}].
loaded_schemas() ->
    [Key || #schema{schema_id=Key} <- ets:tab2list(?SCHEMA_TAB)].
