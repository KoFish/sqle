-module(common_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("test.hrl").

common_test_() ->
    [?_eq(<<"SELECT 1 FROM Foo;DELETE FROM Foo">>,
          [sqle:select([1], 'Foo'),
           sqle:delete('Foo')])
    ].
