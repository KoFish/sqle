-module(select_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("test.hrl").

select_test_() ->
    [?_eq(<<"SELECT *">>,
          sqle:select(['*'])),
     ?_eq(<<"SELECT * FROM Table">>,
          sqle:select(['*'], 'Table')),
     ?_eq(<<"SELECT foo,42 FROM Table">>,
          sqle:select(['foo', 42], 'Table')),
     ?_eq(<<"SELECT 'foo',42 FROM Table">>,
          sqle:select([<<"foo">>, 42], 'Table')),
     ?_eq(<<"SELECT 'foo' AS f,42 FROM Table">>,
          sqle:select([{<<"foo">>, 'as', f}, 42], 'Table')),
     ?_eq(<<"SELECT COUNT(*) FROM Table">>,
          sqle:select([{<<"COUNT(*)">>}], 'Table')),
     %% Select with alias for table
     ?_eq(<<"SELECT 'foo',42 FROM Table AS t">>,
          sqle:select([<<"foo">>, 42], {'Table', 'as', t})),
     ?_eq(<<"SELECT NOW(),42 FROM Table AS t">>,
          sqle:select([{<<"NOW()">>}, 42], {'Table', 'as', t})),
     %% Select with join
     ?_eq(<<"SELECT NOW(),42 FROM Table JOIN Foobar AS f">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table', [{joins, [{'Foobar', 'as', f}]}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table LEFT OUTER JOIN Foobar AS f">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table', [{joins, [{{'Foobar', 'as', f}, [left]}]}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table OUTER JOIN Foobar AS f">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table', [{joins, [{{'Foobar', 'as', f}, [outer]}]}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table INNER JOIN Foobar">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table', [{joins, [{'Foobar', [inner]}]}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table NATURAL INNER JOIN Foobar">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table', [{joins, [{'Foobar', [inner, natural]}]}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table JOIN Foobar ON Foobar.foo=Table.bar">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table',
                      [{joins, [{'Foobar', [{on, {{'Foobar', foo}, '=', {'Table', bar}}}]}]}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table JOIN Foobar ON Foobar.foo=Table.bar JOIN Barfoo USING (foo)">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table',
                      [{joins, [{'Foobar', [{on, {{'Foobar', foo}, '=', {'Table', bar}}}]},
                                {'Barfoo', [{using, [foo]}]}]}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table JOIN Foobar ON Foobar.foo=Table.bar OR Foobar.foo IS NULL">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table',
                      [{joins, [{'Foobar', [{on, {'or', [{{'Foobar', foo}, '=', {'Table', bar}},
                                                         {{'Foobar', foo}, 'is', null}]}}]}]}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table JOIN Foobar USING (foo,bar)">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table',
                      [{joins, [{'Foobar', [{using, [foo, bar]}]}]}])),
     %% Select with where
     ?_eq(<<"SELECT NOW(),42 FROM Table WHERE NOT Table.foo IS NULL">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table',
                      [{where, {'not', {{'Table', foo}, 'is', null}}}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table WHERE Foo">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table',
                      [{where, {<<"Foo">>}}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table WHERE Table.foo IS NULL AND Table.foo IN (1,2,3)">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table',
                      [{where, {'and', [{{'Table', foo}, 'is', null},
                                        {{'Table', foo}, 'in', [1,2,3]}]}}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table WHERE Table.foo IS NULL AND Table.foo IN (1,2,3)">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table',
                      [{where, [{{'Table', foo}, 'is', null},
                                {{'Table', foo}, 'in', [1,2,3]}]}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table WHERE NOT (Table.foo IS NULL AND Table.foo IN (1,2,3))">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table',
                      [{where, {'not', [{{'Table', foo}, 'is', null},
                                        {{'Table', foo}, 'in', [1,2,3]}]}}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table WHERE NOT (Table.foo IS NULL AND Table.foo IN (1,2,3))">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table',
                      [{where, {'not', {'and', [{{'Table', foo}, 'is', null},
                                                {{'Table', foo}, 'in', [1,2,3]}]}}}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table WHERE NOT Table.foo IS NULL OR Table.foo IN (1,2,3)">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table',
                      [{where, {'or', [{'not', {{'Table', foo}, 'is', null}},
                                       {{'Table', foo}, 'in', [1,2,3]}]}}])),
     ?_eq(<<"SELECT NOW(),42 FROM Table WHERE (Table.foo IS NULL OR Table.foo IN (1,2,3)) AND 'Foo'=42">>,
          sqle:select([{<<"NOW()">>}, 42], 'Table',
                      [{where, {'and', [{'or', [{{'Table', foo}, 'is', null},
                                                {{'Table', foo}, 'in', [1,2,3]}]},
                                        {<<"Foo">>, '=', 42}]}}])),
     %% Select with limit and offset
     ?_eq(<<"SELECT * FROM Table LIMIT 1">>, sqle:select(['*'], 'Table', [{limit, 1}])),
     ?_eq(<<"SELECT * FROM Table OFFSET 1">>, sqle:select(['*'], 'Table', [{offset, 1}])),
     ?_eq(<<"SELECT * FROM Table LIMIT 1 OFFSET 2">>,
          sqle:select(['*'], 'Table', [{limit, 1}, {offset, 2}])),
     ?_eq(<<"SELECT * FROM Table ORDER BY 1">>,
          sqle:select(['*'], 'Table', [{order_by, 1}])),
     ?_eq(<<"SELECT * FROM Table ORDER BY 1 DESC">>,
          sqle:select(['*'], 'Table', [{order_by, {1, desc}}])),
     ?_eq(<<"SELECT * FROM Table ORDER BY (foo DESC)">>,
          sqle:select(['*'], 'Table', [{order_by, [{foo, desc}]}])),
     ?_eq(<<"SELECT * FROM Table ORDER BY (foo DESC,bar)">>,
          sqle:select(['*'], 'Table', [{order_by, [{foo, desc},bar]}])),
     %% Sub selects etc.
     ?_eq(<<"SELECT * FROM (SELECT * FROM Foo)">>,
          sqle:select(['*'], sqle:select(['*'], 'Foo'))),
     ?_eq(<<"SELECT * FROM (SELECT * FROM Foo) AS T">>,
          sqle:select(['*'], {sqle:select(['*'], 'Foo'), 'as', 'T'})),
     ?_eq(<<"SELECT * FROM Foo WHERE EXISTS (SELECT * FROM Bar)">>,
          sqle:select(['*'], 'Foo', [{where, {'exists', sqle:select(['*'], 'Bar')}}])),
     ?_eq(<<"SELECT * FROM Foo WHERE Foo.f IN (SELECT apa FROM Bar)">>,
          sqle:select(['*'], 'Foo', [{where, {{'Foo', 'f'}, 'in', sqle:select(['apa'], 'Bar')}}])),
     ?_assert(true)
    ].
