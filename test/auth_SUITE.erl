%% Assumes the bare HTTP non-authed path is checked in load_schemas_SUITE
-module(auth_SUITE).
-compile([export_all, nowarn_export_all]).
-include("avrogato.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [basic_auth, crypto_check].

init_per_testcase(_, Config) ->
    process_flag(trap_exit, true),
    start_server(Config).

end_per_testcase(_, Config) ->
    stop_server(Config),
    Config.

basic_auth(Config) ->
    Port = integer_to_list(?config(http_port, Config)),
    URL = "http://user:pass@localhost:" ++ Port ++ "/",
    {Hdrs, Opts} = avrogato_httpc:opts(URL),
    <<"">> = avrogato_httpc:request(URL, Hdrs, Opts),
    receive
        {req, L} ->
            {http_header, _, _, _, Val} = lists:keyfind('Authorization', 3, L),
            "Basic "++B64 = Val,
            ?assertEqual(<<"user:pass">>, base64:decode(B64))
    after 5000 ->
        error(timeout)
    end,
    ok.

%% Checking for certs that work is kind of a huge pain since we do validate
%% them against a public CA root cert bundle, and so we'd need to work with
%% real certs that come with a time bomb to be safe.
%%
%% For now just check that https:// URIs get turned to actual TLS calls.
%% TODO: maybe check that self-signed certs get declared invalid?
crypto_check(Config) ->
    Port = integer_to_list(?config(http_port, Config)),
    URL = "https://user:pass@localhost:" ++ Port ++ "/",
    {Hdrs, Opts} = avrogato_httpc:opts(URL),
    %% the server just does plaintext HTTP, so the failure is expected
    {_, {{bad_http_response, {error, _}},_}} = (catch avrogato_httpc:request(URL, Hdrs, Opts)),
    receive
        {req, {http_error, TLSBlob}} ->
            %% See https://tls.ulfheim.net/ for TLS record structure
            ?assertMatch([16#16, % handshake record
                          16#03, 16#03 % TLS 1.2 (1.3 parades as 1.2 anyway?)
                          | _], % don't care for the rest, we just know crypto takes place
                         TLSBlob)
    after 1000 ->
        error(timeout)
    end,
    ok.

%%%%%%%%%%%%%%%
%%% HELPERS %%%
%%%%%%%%%%%%%%%

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
    loop(Parent, Listen).

loop(Parent, Listen) ->
    case gen_tcp:accept(Listen, 100) of
        {error, timeout} ->
            receive
                stop ->
                    ok
            after 0 ->
                loop(Parent, Listen)
            end;
        {ok, Sock} ->
            Res = handle_conn(Sock, []),
            Parent ! {req, Res},
            loop(Parent, Listen)
    end.

handle_conn(Sock, Acc) ->
    receive
        %% Good Responses
        {http, Sock, {http_request, 'GET',_ ,_} = Req} ->
            gen_tcp:send(Sock, "HTTP/1.1 204 OK\r\n"
                               "Content-Length: 0\r\n\r\n"),
            handle_conn(Sock, [Req|Acc]);
        {http, Sock, {http_header, _, _, _, _} = Hdr} ->
            handle_conn(Sock, [Hdr|Acc]);
        {http, Sock, http_eoh} ->
            lists:reverse(Acc);
        {http, Sock, {http_error, _} = Req} ->
            Req;
        {tcp_closed, Sock} ->
            lists:reverse(Acc);
        stop ->
            ok;
        X ->
            ct:pal("Exiting with bad thing ~p", [X]),
            exit(X)
    after 60000 ->
        gen_tcp:close(Sock),
        exit(timeout)
    end.
