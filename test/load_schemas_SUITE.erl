-module(load_schemas_SUITE).
-compile([export_all, nowarn_export_all]).
-include("avrogato.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("erlavro/include/erlavro.hrl").

all() ->
    [load_dumped_schemas, find_handler, encode_decode,
     unfound_registry, undefined_version].

init_per_suite(Config) ->
    application:load(avrogato),
    application:set_env(avrogato, registry, "http://nevercallthis.localhost:8080"),
    Config.

end_per_suite(Config) ->
    application:unload(avrogato),
    Config.

init_per_testcase(unfound_registry, Config0) ->
    Config = start_server(Config0),
    Port = ?config(http_port, Config),
    application:load(avrogato),
    {ok, Old} = application:get_env(avrogato, registry),
    application:set_env(avrogato, registry, "http://localhost:"++integer_to_list(Port)),
    {ok, Apps} = application:ensure_all_started(avrogato),
    [{apps, Apps}, {old_registry, Old} | Config];
init_per_testcase(load_dumped_schemas, Config) ->
    {ok, Apps} = application:ensure_all_started(avrogato),
    logger:add_handler(?MODULE, ?MODULE, #{config => self()}),
    [{apps, Apps} | Config];
init_per_testcase(_, Config) ->
    {ok, Apps} = application:ensure_all_started(avrogato),
    Cache = filename:join([code:priv_dir(avrogato), "anon.cache"]),
    ok = avrogato_registry:load_schemas(Cache),
    [{apps, Apps} | Config].

end_per_testcase(unfound_registry, Config) ->
    stop_server(Config),
    [application:stop(App) || App <- lists:reverse(?config(apps, Config))],
    Config;
end_per_testcase(load_dumped_schemas, Config) ->
    logger:remove_handler(?MODULE),
    [application:stop(App) || App <- lists:reverse(?config(apps, Config))],
    Config;
end_per_testcase(_, Config) ->
    [application:stop(App) || App <- lists:reverse(?config(apps, Config))],
    Config.

load_dumped_schemas() ->
    [{docs, "Check that we correctly load the schemas from dump files "}].
load_dumped_schemas(_Config) ->
    Cache = filename:join([code:priv_dir(avrogato), "anon.cache"]),
    ok = avrogato_registry:load_schemas(Cache),
    FetchReports = fun Flush() ->
        receive
            #{msg := {report, R}} -> [R|Flush()]
        after 0 ->
            []
        end
    end,
    [R1, R2, R3] = FetchReports(),
    ct:pal("Failure reports: ~p", [[R1,R2,R3]]),
    %% Bad test schema on first version used forbidden names
    ?assertMatch(#{what := {schema, {"test.test_event", 1}},
                   reason := {error,{reserved,<<"enum">>,<<"enum">>}}},
                 R1),
    %% Schemas that were published/merged in events repo but actually
    %% were broken or badly specified
    ?assertMatch(#{what := {schema, {"fykzydwmwmtb.fbqeewizycgqrm",1}}}, R2),
    ?assertMatch(#{what := {schema, {"zcpcejkz.lrowswnmgwhqe",3}}}, R3),

    %% bad one not inserted
    ?assertException(error, bad_schema,
                     avrogato_registry:lookup_schema(<<"test.test_event">>, 1)),
    %% but other versions are in
    _ = avrogato_registry:lookup_schema(<<"test.test_event">>, 2),
    _ = avrogato_registry:lookup_schema(<<"test.test_event">>, 3),
    _ = avrogato_registry:lookup_schema(<<"test.test_event">>, 4),
    _ = avrogato_registry:lookup_schema(<<"test.test_event">>, 5),
    Older = avrogato_registry:lookup_schema(<<"test.test_event">>, 6),
    Newest = avrogato_registry:lookup_schema(<<"test.test_event">>, 7),
    Newest = avrogato_registry:lookup_schema(<<"test.test_event">>),
    ?assertNotEqual(Older, Newest),

    %% check for other names, fetched by latest version:
    avrogato_registry:lookup_schema(<<"ajulrhrcuezd.hjlpqkxi">>),
    avrogato_registry:lookup_schema(<<"upqghmroppgma.bnrpbiff">>),
    avrogato_registry:lookup_schema(<<"ohlsnepsfi.mvduouagyeesrfr">>),
    avrogato_registry:lookup_schema(<<"pozhjpltxtnk.dqkouqmth">>),
    avrogato_registry:lookup_schema(<<"ndxwgzjsk.reuwnfqfmawinnz">>),
    ok.

find_handler() ->
    [{docs, "Check that the base components for encoding/decoding "
            "are made available for each message"}].
find_handler(_Config) ->
    Handler = avrogato_registry:lookup_schema(<<"gcgxoxqspeoh.zimotyxipxv">>),
    {Encoder, _ConfluentHeader, OCFHeader} = Handler,
    Event = event(<<"gcgxoxqspeoh.zimotyxipxv">>),
    Bin = Encoder(<<"gcgxoxqspeoh.zimotyxipxv">>, Event),
    OCFEncoded = iolist_to_binary(avro_ocf:make_ocf(OCFHeader, [Bin])),
    {_Header, Record, [T]} = avro_ocf:decode_binary(OCFEncoded),
    #avro_record_type{fullname = <<"gcgxoxqspeoh.zimotyxipxv">>} = Record,
    ?assertEqual(avrogato_test_utils:sort_record(Event),
                 avrogato_test_utils:sort_record(T)),
    ok.

encode_decode() ->
    [{doc, "Test the circularity of encoding/decoding functions"}].
encode_decode(_Config) ->
    Event = event(<<"gcgxoxqspeoh.zimotyxipxv">>),
    ?assertEqual(
         {error, schema_mismatch},
         avrogato:decode(
             {<<"gcgxoxqspeoh.zimotyxipxv">>, 3},
             avrogato:encode({<<"gcgxoxqspeoh.zimotyxipxv">>, 4}, Event)
         )
    ),
    {ok, Roundtripped} = avrogato:decode(
        {<<"gcgxoxqspeoh.zimotyxipxv">>, 4},
        avrogato:encode({<<"gcgxoxqspeoh.zimotyxipxv">>, 4}, Event)
    ),
    ?assertEqual(avrogato_test_utils:sort_record(Event),
                 avrogato_test_utils:sort_record(Roundtripped)),
    ok.

unfound_registry() ->
    [{doc, "Check the failure path when a registry can't be found"}].
unfound_registry(_Config) ->
    ?assertMatch(ok, avrogato_registry:sync_id(1)),
    ?assertMatch(ok, avrogato_registry:sync_schema(<<"f.goodschema">>)),
    ?assertMatch({error, {bad_http_response, _}}, avrogato_registry:sync()),
    ?assertMatch({error, {bad_http_response, _}}, avrogato_registry:sync_id(-1)),
    ?assertMatch({error, {bad_http_response, _}}, avrogato_registry:sync_schema(<<"badschema">>)),
    ?assertException(error, bad_id, avrogato_registry:lookup_id(-1)),
    ?assertException(error, bad_schema, avrogato_registry:lookup_schema(<<"badschema">>)),
    ok.

undefined_version() ->
    [{doc, "When adding a schema by ID, it internally has an undefined "
           "version, wich needs explicit handling when decoding mismatches"}].
undefined_version(_Config) ->
    Event = event(<<"gcgxoxqspeoh.zimotyxipxv">>),
    Id = avrogato_registry:find_schema_id(<<"gcgxoxqspeoh.zimotyxipxv">>, 4),
    [Row=#id{schema_id={Name, _}}] = ets:lookup(?ID_TAB, Id),
    true = ets:insert(?ID_TAB, Row#id{schema_id={Name, undefined}}),
    ?assertEqual(
         {error, schema_mismatch},
         avrogato:decode(
             {<<"gcgxoxqspeoh.zimotyxipxv">>, 3},
             avrogato:encode({<<"gcgxoxqspeoh.zimotyxipxv">>, 4}, Event)
         )
    ),
    %% But it won't show up for OCF since it won't carry version
    ?assertMatch(
         {ok, _},
         avrogato:decode(
             {<<"gcgxoxqspeoh.zimotyxipxv">>, 3},
             avrogato:encode_ocf({<<"gcgxoxqspeoh.zimotyxipxv">>, 4}, Event)
         )
    ),
    ok.

%%% Helpers
event(<<"gcgxoxqspeoh.zimotyxipxv">>) ->
    [
        {<<"ulxhimtkddnzsz">>, <<"originator">>},
        {<<"rhaxho">>, os:system_time(milli_seconds)},
        {<<"mclcqntgvyjbhbw">>, <<"123e4567-e89b-12d3-a456-426655440000">>},
        {<<"mkuodyhtb">>, 1},
        {<<"gydsrhuhcnql">>, [
            {<<"jwtpyiafbklp">>, <<"failing">>},
            {<<"simhmrisonthz">>, <<"123456">>},
            {<<"hllqqgweierz">>, <<"123e4567-e89b-12d3-a456-426655440000">>},
            {<<"cjgafkopgncbjc">>, <<"30006">>}
        ]}
    ].

%% log handler callback
log(Event, #{config := Pid}) ->
    Pid ! Event.

start_server(Config) ->
    Parent = self(),
    [{http_serv, spawn_link(fun() -> server(Parent) end)},
     {http_port, receive {port, Port} -> Port end}
     | Config].

stop_server(Config) ->
    ?config(http_serv, Config) ! stop,
    application:set_env(avrogato, registry, ?config(old_registry, Config)).

server(Parent) ->
    {ok, Listen} = gen_tcp:listen(0, [{active, true}, {packet, http}]),
    {ok, Port} = inet:port(Listen),
    Parent ! {port, Port},
    loop(Listen).

loop(Listen) ->
    case gen_tcp:accept(Listen, 100) of
        {error, timeout} ->
            receive
                stop ->
                    ok
            after 0 ->
                loop(Listen)
            end;
        {ok, Sock} ->
            handle_conn(Sock),
            loop(Listen)
    end.

handle_conn(Sock) ->
    receive
        %% Bad Responses
        {http, Sock, {http_request,'GET',{abs_path,"/schemas/ids/-1"},_}} ->
            gen_tcp:send(Sock, "HTTP/1.1 404 NOT FOUND\r\n"
                               "Content-Length: 0\r\n\r\n"),
            handle_conn(Sock);
        {http, Sock, {http_request,'GET',{abs_path,"/subjects/badschema/versions"},_}} ->
            gen_tcp:send(Sock, "HTTP/1.1 404 NOT FOUND\r\n"
                               "Content-Length: 0\r\n\r\n"),
            handle_conn(Sock);
        {http, Sock, {http_request,'GET',{abs_path,"/subjects"},_}} ->
            gen_tcp:send(Sock, "HTTP/1.1 404 NOT FOUND\r\n"
                               "Content-Length: 0\r\n\r\n"),
            handle_conn(Sock);
        %% Good Responses
        {http, Sock, {http_request,'GET',{abs_path,"/schemas/ids/1"},_}} ->
            Body = json_body(id),
            gen_tcp:send(Sock, "HTTP/1.1 200 OK\r\n"
                               "Content-Type: application/json; charset=UTF-8\r\n"
                               "Content-Length: "++ integer_to_list(string:length(Body)) ++"\r\n"
                               "\r\n" ++ Body),
            handle_conn(Sock);
        {http, Sock, {http_request,'GET',{abs_path,"/subjects/f.goodschema/versions"},_}} ->
            gen_tcp:send(Sock, "HTTP/1.1 200 OK\r\n"
                               "Content-Type: application/json; charset=UTF-8\r\n"
                               "Content-Length: 3\r\n"
                               "\r\n[1]"),
            handle_conn(Sock);
        {http, Sock, {http_request,'GET',{abs_path,"/subjects/f.goodschema/versions/1"},_}} ->
            Body = json_body(schema),
            gen_tcp:send(Sock, "HTTP/1.1 200 OK\r\n"
                               "Content-Type: application/json; charset=UTF-8\r\n"
                               "Content-Length: "++ integer_to_list(string:length(Body)) ++"\r\n"
                               "\r\n" ++ Body),
            handle_conn(Sock);
        {http, Sock, {http_header, _, _, _, _}=_Hdr} ->
            handle_conn(Sock);
        {http, Sock, http_eoh} ->
            handle_conn(Sock);
        {tcp_closed, Sock} ->
            ok;
        stop ->
            ok;
        X ->
            exit(X)
    after 60000 ->
        gen_tcp:close(Sock),
        exit(timeout)
    end.

json_body(id) ->
    "{\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"goodschema\\\", \\\"namespace\\\":\\\"f\\\",\\\"fields\\\": [{\\\"name\\\":\\\"data\\\",\\\"type\\\":{\\\"type\\\":\\\"record\\\", \\\"name\\\":\\\"login_data\\\",\\\"fields\\\": [{\\\"name\\\": \\\"success\\\",\\\"type\\\":\\\"boolean\\\"}, {\\\"name\\\":\\\"user_email\\\",\\\"type\\\":\\\"string\\\"}, {\\\"name\\\": \\\"user_id\\\",\\\"type\\\":\\\"long\\\"}, {\\\"name\\\":\\\"x_header_request\\\",\\\"type\\\":[\\\"null\\\",\\\"string\\\"], \\\"default\\\":null}]}}, {\\\"name\\\":\\\"ts\\\",\\\"type\\\":{\\\"type\\\":\\\"long\\\",\\\"logicalType\\\":\\\"timestamp-millis\\\"}}, {\\\"name\\\":\\\"uuid\\\",\\\"type\\\":{\\\"type\\\":\\\"string\\\",\\\"logicalType\\\":\\\"uuid\\\"}}, {\\\"name\\\":\\\"version\\\",\\\"type\\\":\\\"int\\\"}]}\"}";
json_body(schema) ->
    "{\"id\":1,\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"goodschema\\\", \\\"namespace\\\":\\\"f\\\",\\\"fields\\\": [{\\\"name\\\":\\\"data\\\",\\\"type\\\":{\\\"type\\\":\\\"record\\\", \\\"name\\\":\\\"login_data\\\",\\\"fields\\\": [{\\\"name\\\": \\\"success\\\",\\\"type\\\":\\\"boolean\\\"}, {\\\"name\\\":\\\"user_email\\\",\\\"type\\\":\\\"string\\\"}, {\\\"name\\\": \\\"user_id\\\",\\\"type\\\":\\\"long\\\"}, {\\\"name\\\":\\\"x_header_request\\\",\\\"type\\\":[\\\"null\\\",\\\"string\\\"], \\\"default\\\":null}]}}, {\\\"name\\\":\\\"ts\\\",\\\"type\\\":{\\\"type\\\":\\\"long\\\",\\\"logicalType\\\":\\\"timestamp-millis\\\"}}, {\\\"name\\\":\\\"uuid\\\",\\\"type\\\":{\\\"type\\\":\\\"string\\\",\\\"logicalType\\\":\\\"uuid\\\"}}, {\\\"name\\\":\\\"version\\\",\\\"type\\\":\\\"int\\\"}]}\"}".
