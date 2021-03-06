-module(common_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("test.hrl").

common_test_() ->
    [?_eq(<<"SELECT 1 FROM Foo;DELETE FROM Foo">>,
          [sqle:select([1], 'Foo'),
           sqle:delete('Foo')]),
     ?_eeq({<<"SELECT $1">>, [1]},
           sqle:select([1])),
     ?_eeq({<<"SELECT $1,$2 FROM Foo">>, [<<"test">>, 42]},
           sqle:select([<<"test">>, 42], 'Foo')),
     ?_eeq({<<"SELECT $1 FROM Foo WHERE EXISTS (SELECT $2 FROM Table WHERE apa=$3 AND bepa=$4)">>,
            [100, 200, 300, 400]},
           sqle:select([100], 'Foo', [{where, {'exists', sqle:select([200], 'Table',
                                                                     [{where, {'and', [{apa, '=', 300},
                                                                                       {bepa, '=', 400}]}}])}}
                                     ]))
    ].
