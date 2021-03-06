-module(insert_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("test.hrl").

insert_test_() ->
    [?_eq(<<"INSERT INTO Foo (a,b) DEFAULT VALUES">>,
          sqle:insert('Foo', default_values, [{columns, [a, b]}])),
     ?_eq(<<"INSERT INTO Foo DEFAULT VALUES">>,
          sqle:insert('Foo', default_values)),
     ?_eq(<<"INSERT INTO Foo VALUES (SELECT a,b FROM Bar)">>,
          sqle:insert('Foo', [sqle:select([a, b], 'Bar')])),
     ?_eq(<<"INSERT INTO Foo VALUES ('APA',42)">>,
          sqle:insert('Foo', [[<<"APA">>, 42]])),
     ?_eq(<<"INSERT INTO Foo VALUES ('APA',42),('BEPA',43)">>,
          sqle:insert('Foo', [[<<"APA">>, 42], [<<"BEPA">>, 43]])),
     ?_eq(<<"INSERT INTO Foo (a,b,c) VALUES ('APA',42,NOW())">>,
          sqle:insert('Foo', [[<<"APA">>, 42, {lit, <<"NOW()">>}]],
                     [{columns, [a, b, c]}])),
     ?_eq(<<"INSERT INTO Foo AS F (a,b) VALUES ('APA',42)">>,
          sqle:insert({'Foo', 'as', 'F'}, [[<<"APA">>, 42]],
                     [{columns, [a, b]}])),
     ?_eq(<<"INSERT INTO Foo AS F VALUES ('APA',42) RETURNING foo">>,
          sqle:insert({'Foo', 'as', 'F'}, [[<<"APA">>, 42]],
                      [{returning, [foo]}])),
     ?_eq(<<"INSERT INTO Foo VALUES ('APA',42) RETURNING c,d">>,
          sqle:insert('Foo', [[<<"APA">>, 42]],
                      [{returning, [c, d]}])),
     ?_eq(<<"INSERT INTO Foo VALUES ('APA',42) ON CONFLICT DO NOTHING">>,
          sqle:insert('Foo', [[<<"APA">>, 42]],
                      [{on_conflict, do_nothing}])),
     ?_eq(<<"INSERT INTO Foo VALUES ('APA',42) ON CONFLICT DO UPDATE SET a=1">>,
          sqle:insert('Foo', [[<<"APA">>, 42]],
                      [{on_conflict, {do_update, [{a, 1}]}}])),
     ?_eq(<<"INSERT INTO Foo VALUES ('APA',42) ON CONFLICT DO UPDATE SET a=1,b=2">>,
          sqle:insert('Foo', [[<<"APA">>, 42]],
                      [{on_conflict, {do_update, [{a, 1},{b, 2}]}}])),
     ?_eq(<<"INSERT INTO Foo VALUES ('APA',42) ON CONFLICT DO UPDATE SET a=DEFAULT">>,
          sqle:insert('Foo', [[<<"APA">>, 42]],
                      [{on_conflict, {do_update, [{a, default}]}}])),
     ?_eq(<<"INSERT INTO Foo VALUES ('APA',42) ON CONFLICT DO UPDATE SET a=excluded.a">>,
          sqle:insert('Foo', [[<<"APA">>, 42]],
                      [{on_conflict, {do_update, [{a, {excluded, a}}]}}])),
     ?_assert(true)].
