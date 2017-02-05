-module(delete_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("test.hrl").

delete_test_() ->
    [?_eq(<<"DELETE FROM Foo">>,
          sqle:delete('Foo')),
     ?_eq(<<"DELETE FROM Foo WHERE b=10">>,
          sqle:delete('Foo', [{where, {b, '=', 10}}])),
     ?_eq(<<"DELETE FROM Foo AS f">>,
          sqle:delete({'Foo', 'as', f})),
     ?_eq(<<"DELETE FROM Foo WHERE a=10 OR b=20 RETURNING a,b">>,
          sqle:delete('Foo', [{where, {'or', [{a, '=', 10}, {b, '=', 20}]}},
                              {returning, [a,b]}])),
     ?_eq(<<"DELETE FROM Foo RETURNING a,b">>,
          sqle:delete('Foo', [{returning, [a,b]}])),
     ?_eq(<<"DELETE FROM Foo RETURNING a AS foobar,b">>,
          sqle:delete('Foo', [{returning, [{a, 'as', foobar}, b]}])),
     ?_assert(true)].
