%%% @private
%%% avrogato's app behaviour
%%% @end

-module(avrogato_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    avrogato_sup:start_link().

stop(_State) ->
    ok.
