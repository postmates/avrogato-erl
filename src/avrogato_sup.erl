%%%-------------------------------------------------------------------
%% @doc avrogato top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(avrogato_sup).
-behaviour(supervisor).
-include("avrogato.hrl").

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    %% Own the ETS table for the whole app. This forces is to be public,
    %% but allows the worker handling it to be transient.
    ets:new(?SCHEMA_TAB, [named_table, ordered_set, public,
                          {keypos, #schema.schema_id},
                          {read_concurrency, true}]),
    ets:new(?ID_TAB, [named_table, set, public,
                      {keypos, #id.id},
                      {read_concurrency, true}]),
    SupFlags = #{strategy => one_for_all,
                 intensity => 1,
                 period => 10},
    Registry = case application:get_env(avrogato, registry) of
        {ok, Entry} -> Entry;
        undefined -> error({undefined_app_env, {avrogato, registry}})
    end,
    Prefetch = application:get_env(avrogato, prefetch, []),
    ChildSpecs = [
        #{id => avrogato_registry,
          start => {avrogato_registry,
                    start_link,
                    [Registry, Prefetch]},
          restart => permanent,
          shutdown => 30000,
          type => worker
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.

