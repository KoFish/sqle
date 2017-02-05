-module(update_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("test.hrl").

update_test_() ->
    [?_eq(<<"UPDATE Foo SET a=42">>,
          sqle:update('Foo', [{a, 42}])),
     ?_eq(<<"UPDATE Foo SET a=42,b=10">>,
          sqle:update('Foo', [{a, 42}, {b, 10}])),
     ?_eq(<<"UPDATE Foo SET a=42 WHERE b=10">>,
          sqle:update('Foo', [{a, 42}], [{where, {b, '=', 10}}])),
     ?_eq(<<"UPDATE Foo SET a=a*10">>,
          sqle:update('Foo', [{a, {a, '*', 10}}])),
     ?_eq(<<"UPDATE Foo SET a=10 RETURNING a,b">>,
          sqle:update('Foo', [{a, 10}], [{returning, [a,b]}])),
     ?_assert(true)].
