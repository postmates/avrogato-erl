-module(prop_avrogato).
-include("avrogato.hrl").
-include_lib("proper/include/proper.hrl").

-include_lib("erlavro/include/erlavro.hrl").
-include_lib("erlavro/include/avro_internal.hrl").

%%%%%%%%%%%%%%%%%%
%%% Properties %%%
%%%%%%%%%%%%%%%%%%
prop_confluent() ->
    ?SETUP(prepare(),
    ?FORALL({Type, Record}, event(),
      ?WHENFAIL(
        io:format(
          "FAILURE:~n~p~n=> ~p~n",
          [{Type, Record},
            catch avrogato:decode(Type, avrogato:encode(Type, Record))]
        ),
        begin
            {ok, Decoded} = avrogato:decode(Type, avrogato:encode(Type, Record)),
            avrogato_test_utils:records_equal(Record, Decoded)
        end))
    ).

prop_ocf() ->
    ?SETUP(prepare(),
    ?FORALL({Type, Record}, event(),
      ?WHENFAIL(
        io:format(
          "FAILURE:~n~p~n=> ~p~n",
          [{Type, Record},
            catch avrogato:decode(Type, avrogato:encode_ocf(Type, Record))]
        ),
        begin
            {ok, Decoded} = avrogato:decode(Type, avrogato:encode_ocf(Type, Record)),
            avrogato_test_utils:records_equal(Record, Decoded)
        end))
    ).

prop_wildcard() ->
    ?SETUP(prepare(),
    ?FORALL({Fmt, {Type={Name,_Vsn}, Record}},
            {oneof([confluent, ocf]), event()},
      ?WHENFAIL(
        io:format(
          "FAILURE:~n~p~n=> ~p~n",
          [{Type, Record},
            catch {avrogato:decode_any(avrogato:encode(Type, Record)),
                   avrogato:decode_any(avrogato:encode_ocf(Type, Record))}]
        ),
        begin
            Encoded = case Fmt of
                confluent -> avrogato:encode(Type, Record);
                ocf -> avrogato:encode_ocf(Type, Record)
            end,
            {Name, Decoded} = avrogato:decode_any(Encoded),
            avrogato_test_utils:records_equal(Record, Decoded)
        end))
    ).

%%%%%%%%%%%%%%%%%%
%%% Generators %%%
%%%%%%%%%%%%%%%%%%
event() ->
    ?LET(Schema, schema_name(),
         begin
            {BinName, Vsn} = Schema,
            Id = avrogato_registry:find_schema_id(BinName, Vsn),
            [#id{id=Id, schema=RawSchema}] = ets:lookup(?ID_TAB, Id),
            NewStore = avro_schema_store:new([{dict, true}]),
            SchemaName = <<"proptest.", BinName/binary>>,
            {dict, Dict} = avro_schema_store:add_type(SchemaName, RawSchema, NewStore),
            StoreGen = make_store_gen(BinName, Dict),
            ?LET(Rec, StoreGen, {Schema, Rec})
         end).

schema_name() ->
    oneof(avrogato:loaded_schemas()).

make_store_gen(Key, Dict) ->
    generate_avro(dict:fetch(Key, Dict), Dict).


generate_avro(#avro_primitive_type{name=T, custom=[]}, _Dict) ->
    avro_primitive(T);
generate_avro(#avro_primitive_type{name=T, custom=_Params}, _Dict) ->
    %% just disregard the logical type information and go for the base
    avro_primitive(T);
generate_avro(#avro_record_type{name=_Name, fields=Fields}, Dict) ->
    expand_fields(Fields, Dict);
generate_avro(#avro_enum_type{symbols=Symbols}, _Dict) ->
    oneof(Symbols);
generate_avro(#avro_array_type{type = T}, Dict) ->
    list(generate_avro(T, Dict));
generate_avro(#avro_map_type{type = T}, Dict) ->
    list({utf8(), generate_avro(T, Dict)});
generate_avro(#avro_union_type{id2type = Tree}, Dict) ->
    Types = gb_trees:values(Tree),
    ?LET(Type, oneof(Types), generate_avro(Type, Dict));
generate_avro(#avro_fixed_type{size = Size}, _Dict) ->
    binary(Size);
generate_avro(TypeName, Dict) when is_binary(TypeName) ->
    generate_avro(dict:fetch(TypeName, Dict), Dict).

avro_primitive(?AVRO_NULL) -> null;
avro_primitive(?AVRO_BOOLEAN) -> boolean();
avro_primitive(?AVRO_INT) -> range(-2147483648, 2147483647);
avro_primitive(?AVRO_LONG) -> range(-9223372036854775808, 9223372036854775807);
avro_primitive(?AVRO_FLOAT) -> ?LET(X, float(), begin <<Y:32/float>> = <<X:32/float>>, Y end);
avro_primitive(?AVRO_DOUBLE) -> float();
avro_primitive(?AVRO_BYTES) -> binary();
avro_primitive(?AVRO_STRING) -> utf8().

expand_fields(List, Dict) ->
    [expand_field(X, Dict) || X <- List].

expand_field(#avro_record_field{name=N, type=T}, Dict) ->
    {N, generate_avro(T, Dict)}.

%%%%%%%%%%%%%%%
%%% Helpers %%%
%%%%%%%%%%%%%%%
prepare() ->
    %% Start the app before generation so all the schemas are available
    %% at any time whatsoever
    LogFilter = {fun(#{msg := {report, #{in := "avrogato_schema"}}}, _) -> stop;
                   (_, _) -> ignore
                 end, noarg},
    application:load(avrogato),
    application:set_env(avrogato, registry, "http://nevercallthis.localhost:8080"),
    {ok, Apps} = application:ensure_all_started(avrogato),
    Cache = filename:join([code:priv_dir(avrogato), "anon.cache"]),
    logger:add_primary_filter(?MODULE, LogFilter),
    ok = avrogato_registry:load_schemas(Cache),
    fun() ->
        {ok, _} = application:ensure_all_started(avrogato),
        logger:add_primary_filter(?MODULE, LogFilter),
        ok = avrogato_registry:load_schemas(Cache),
        fun() ->
            logger:remove_primary_filter(?MODULE),
            [application:stop(App) || App <- lists:reverse(Apps)],
            application:unload(avrogato),
            ok
        end
    end.
