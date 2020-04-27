-module(avrogato_test_utils).
-export([records_equal/2, sort_record/1]).

records_equal(Rec1, Rec2) ->
    sort_record(Rec1) =:= sort_record(Rec2).

sort_record(R) ->
    lists:sort(lists:map(fun sort_sub_records/1, R)).

sort_sub_records({Key, Val}) when is_list(Val) ->
    {Key, sort_record(Val)};
sort_sub_records(Field) ->
    Field.
