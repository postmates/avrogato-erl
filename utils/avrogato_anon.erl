%%% @doc
%%% Take and convert a registry dump to be anonymized in terms of all
%%% the field names, defaults, and schema namespaces.
%%% @end
-module(avrogato_anon).
-export([run/2]).

run(InFile, OutFile) ->
    {ok, Bin} = file:read_file(InFile),
    AnonDump = [anon(X) || X <- binary_to_term(Bin)],
    file:write_file(OutFile, term_to_binary(AnonDump, [compressed])).

anon(Entry={{"test.test_event", _}, _}) ->
    %% bypass these
    Entry;
anon({{_Name, Vsn}, {Id, JSON}}) ->
    Map = jsone:decode(JSON),
    Anon = #{<<"name">> := N, <<"namespace">> := NS} = anon_map(Map),
    {{binary_to_list(NS) ++ "." ++ binary_to_list(N), Vsn},
     {Id, jsone:encode(Anon)}}.

anon_map(M) ->
    maps:fold(fun anon_item/3, #{}, M).

anon_item(K, _, M) when K == <<"name">>; K == <<"doc">>; K == <<"namespace">> ->
    M#{K => rand_bin()};
anon_item(<<"default">>, <<_/binary>>, M) ->
    M#{<<"default">> => rand_bin()};
anon_item(K, V, M) when is_map(V) ->
    M#{K => anon_map(V)};
anon_item(K, V, M) when is_list(V) ->
    M#{K => [case is_map(X) of
                 true -> anon_map(X);
                 false -> X
             end || X <- V]};
anon_item(K, V, M) ->
    M#{K => V}.

rand_bin() ->
    First = chr(rand:uniform(26)-1),
    Len = rand:uniform(10)+4,
    <<First, (<< <<(chr(rand:uniform(26)-1))>> || _ <- lists:seq(1, Len)>>)/binary>>.

chr(N) when N >= 0; N =< 25 -> $a+N;
chr(26) -> $_.
