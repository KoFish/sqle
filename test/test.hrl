basic_encoder(X) ->
    R = basic_encoder_(X),
    io:format(" -!!!- Encode ~p into \"~s\"~n", [X, R]),
    R.

basic_encoder_(B) when is_binary(B) -> io_lib:format("'~s'", [B]);
basic_encoder_(L) when is_list(L) -> [$(, intersperse($,, lists:map(fun basic_encoder/1, L)), $)];
basic_encoder_(X) -> unicode:characters_to_binary(io_lib:write(X)).

intersperse(_, []) -> [];
intersperse(Ch, L) -> tl(lists:append([[Ch, E] || E <- L])).

-define(_eq(E, A), ?_assertEqual(E, sqle:to_binary(A, fun basic_encoder/1))).
-define(_eeq(E, A), ?_assertEqual(E, (fun ({C, D}) -> {iolist_to_binary(C), D} end)(sqle:to_equery(A)))).

