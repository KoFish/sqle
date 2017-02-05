-module(sqle).

%% API exports
-export([select/1, select/2, select/3]).
-export([insert/2, insert/3]).
-export([delete/1, delete/2]).
-export([update/2, update/3]).
-export([join/1, join/2]).
-export([alter/3]).
-export([add/3]).
-export([to_iolist/2]).
-export([to_binary/2]).

%% Query object definitions
-define(SELECT(Cols), #{
                 query => select,
                 cols => Cols,
                 table => undefined,
                 joins => undefined,
                 where => undefined,
                 limit => undefined,
                 offset => undefined,
                 order_by => undefined
                }
       ).
-define(INSERT(Into, Values), #{
                       query => insert,
                       into => Into,
                       cols => undefined,
                       vals => Values,
                       on_conflict => undefined,
                       returning => undefined
                      }
       ).
-define(UPDATE(Table, Set), #{
                        query => update,
                        table => Table,
                        vals => Set,
                        where => undefined,
                        returning => undefined
                       }
       ).
-define(DELETE(From), #{
                 query => delete,
                 from => From,
                 where => undefined,
                 returning => undefined
                }
       ).

-define(JOIN(Table), #{
               querypart => join,
               dir => undefined,
               natural => false,
               using => [],
               table => Table,
               on => undefined
              }
       ).

%%====================================================================
%% API functions
%%====================================================================

%% SELECT -----------------------------------------------------------------------------------------
select(Cols) ->
    ?SELECT(Cols).

select(Cols, From) ->
    set_table(select(Cols), From).

select(Cols, From, Opts) ->
    Q = select(Cols, From),
    maps:fold(fun set_opt/3, Q, opts(Opts)).

%% INSERT -----------------------------------------------------------------------------------------
insert(Into, Values) ->
    ?INSERT(Into, Values).

insert(Into, Values, Opts) ->
    Q = insert(Into, Values),
    maps:fold(fun set_opt/3, Q, opts(Opts)).

%% DELETE------------------------------------------------------------------------------------------
delete(From) ->
    ?DELETE(From).

delete(From, Opts) ->
    Q = delete(From),
    maps:fold(fun set_opt/3, Q, opts(Opts)).

%% UPDATE -----------------------------------------------------------------------------------------
update(Table, Set) ->
    ?UPDATE(Table, Set).

update(Table, Set, Opts) ->
    Q = update(Table, Set),
    maps:fold(fun set_opt/3, Q, opts(Opts)).

%% JOIN -------------------------------------------------------------------------------------------
join(Table) ->
    ?JOIN(Table).

join(Table, Opts) ->
    J = join(Table),
    maps:fold(fun set_opt/3, J, opts(Opts)).

%% ALTER operation --------------------------------------------------------------------------------
alter(What, How, Q = #{query := Type})
  when Type =:= select; Type =:= update; Type =:= insert; Type =:= delete ->
    case maps:is_key(What, maps:without([query], Q)) of
        true -> alter_query(What, How, Q);
        false -> throw({invalid_field, What})
    end;
alter(_, _, Q) ->
    throw({invalid_query, Q}).

add(join, {Table, Opts}, Q = #{query := select}) ->
    add(join, join(Table, Opts), Q);
add(join, #{querypart := join} = Join, Q = #{query := select}) ->
    alter(joins, fun (undefined) -> [Join];
                     (OldJoins) -> OldJoins ++ [Join] end, Q);
add(join, Table, Q = #{query := select}) ->
    add(join, join(Table), Q);
add(where, {AndOr, [_ | _] = Cond}, Q = #{query := Query})
  when Query =:= select; Query =:= delete; Query =:= update ->
    alter(where,
          fun (undefined) -> {AndOr, Cond};
              ({AndOr0, L}) when AndOr0 =:= AndOr -> {AndOr, L ++ Cond};
              (OldCond) -> {AndOr, [OldCond] ++ Cond}
          end, Q).


%% SELECT -----------------------------------------------------------------------------------------
to_iolist(#{query := select, cols := Cols} = Q, Enc) ->
    sp([<<"SELECT">>, cols_to_bin(Cols, Enc)]
       ++ select_to_bin(from, {maps:get(table, Q), maps:get(joins, Q)}, Enc)
       ++ select_to_bin(where, maps:get(where, Q), Enc)
       ++ select_to_bin(limit, maps:get(limit, Q), Enc)
       ++ select_to_bin(offset, maps:get(offset, Q), Enc)
       ++ select_to_bin(order_by, maps:get(order_by, Q), Enc)
      );
%% INSERT -----------------------------------------------------------------------------------------
to_iolist(#{query := insert, into := Into, vals := Vals} = Q, Enc) ->
    sp([<<"INSERT">>]
       ++ insert_to_bin(into, Into, Enc)
       ++ insert_to_bin(cols, maps:get(cols, Q), Enc)
       ++ insert_to_bin(vals, Vals, Enc)
       ++ insert_to_bin(on_conflict, maps:get(on_conflict, Q), Enc)
       ++ insert_to_bin(returning, maps:get(returning, Q), Enc)
      );
%% DELETE -----------------------------------------------------------------------------------------
to_iolist(#{query := delete, from := Table} = Q, Enc) ->
    sp([<<"DELETE">>, <<"FROM">>, table_to_bin(Table, Enc)]
       ++ delete_to_bin(where, maps:get(where, Q), Enc)
       ++ delete_to_bin(returning, maps:get(returning, Q), Enc)
      );
%% UPDATE -----------------------------------------------------------------------------------------
to_iolist(#{query := update, table := Table, vals := Set} = Q, Enc) ->
    sp([<<"UPDATE">>, table_to_bin(Table, Enc)]
       ++ update_to_bin(set, Set, Enc)
       ++ update_to_bin(where, maps:get(where, Q), Enc)
       ++ update_to_bin(returning, maps:get(returning, Q), Enc)
      );
%% JOIN -------------------------------------------------------------------------------------------
to_iolist(#{querypart := join, table := Table} = J, Enc) ->
    io:format("J: ~p~n", [J]),
    Dir = maps:get(dir, J),
    sp(
      lists:append(
        [[<<"NATURAL">>] || maps:get(natural, J) =:= true]
        ++ [[<<"INNER">>] || maps:get(dir, J) =:= inner]
        ++ [case Dir of
               {left, _} -> [<<"LEFT">>];
               {right, _} -> [<<"RIGHT">>];
               {full, _} -> [<<"FULL">>];
               _ -> []
           end]
        ++ [[<<"OUTER">>] || element(2, Dir) =:= outer]
        ++ [[<<"JOIN">>, table_to_bin(Table, Enc)]]
        ++ [[<<"USING">>, [$(, cm([b(U) || U <- maps:get(using, J)]), $)]] || maps:get(using, J) =/= []]
        ++ [[<<"ON">>, cond_to_bin(maps:get(on, J), Enc)] || maps:get(on, J) =/= undefined]
       )
     );
%% LIST OF STATEMENTS -----------------------------------------------------------------------------
to_iolist(List, Enc) when is_list(List) ->
    intersperse($;, [to_iolist(E, Enc) || E <- List]).

to_binary(Q, Enc) ->
    iolist_to_binary(to_iolist(Q, Enc)).

%%====================================================================
%% Internal functions
%%====================================================================

set_table(#{query := select} = Q, From) ->
    Q#{table := From}.

%% SELECT -----------------------------------------------------------------------------------------
set_opt(where, Where, #{query := select} = Q) ->
    Q#{where := Where};
set_opt(limit, Limit, #{query := select} = Q) ->
    Q#{limit := Limit};
set_opt(offset, Offset, #{query := select} = Q) ->
    Q#{offset := Offset};
set_opt(order_by, Order, #{query := select} = Q) ->
    Q#{order_by := Order};
set_opt(joins, Joins, #{query := select} = Q) ->
    Q#{joins := lists:map(
                 fun ({Table, Opts}) ->
                         join(Table, Opts);
                     (#{querypart := join} = Join) ->
                         Join;
                     (Table) ->
                         join(Table)
                 end,
                 Joins)};
%% INSERT -----------------------------------------------------------------------------------------
set_opt(columns, Columns, #{query := insert} = Q) ->
    Q#{cols := Columns};
set_opt(on_conflict, OnConf, #{query := insert} = Q) ->
    Q#{on_conflict := OnConf};
set_opt(returning, Returning, #{query := insert} = Q) ->
    Q#{returning := Returning};
%% DELETE -----------------------------------------------------------------------------------------
set_opt(where, Where, #{query := delete} = Q) ->
    Q#{where := Where};
set_opt(returning, Returning, #{query := delete} = Q) ->
    Q#{returning := Returning};
%% UPDATE -----------------------------------------------------------------------------------------
set_opt(where, Where, #{query := update} = Q) ->
    Q#{where := Where};
set_opt(returning, Returning, #{query := update} = Q) ->
    Q#{returning := Returning};
%% JOIN -------------------------------------------------------------------------------------------
set_opt(inner, true, #{querypart := join} = J) ->
    J#{dir := inner};
set_opt(outer, true, #{querypart := join, dir := {_, outer}} = J) ->
    J;
set_opt(outer, true, #{querypart := join} = J) ->
    J#{dir := {undefined, outer}};
set_opt(left, true, #{querypart := join} = J) ->
    J#{dir := {left, outer}};
set_opt(right, true, #{querypart := join} = J) ->
    J#{dir := {right, outer}};
set_opt(full, true, #{querypart := join} = J) ->
    J#{dir := {full, outer}};
set_opt(natural, Nat, #{querypart := join} = J) ->
    J#{natural := Nat};
set_opt(using, Using, #{querypart := join} = J) ->
    J#{using := Using};
set_opt(on, Cond, #{querypart := join} = J) ->
    J#{on := Cond}.

table_to_bin({T, 'as', A}, Enc) ->
    sp([table_to_bin(T, Enc), <<"AS">>, b(A)]);
table_to_bin(Q = #{query := select}, Enc) ->
    [$(, to_iolist(Q, Enc), $)];
table_to_bin(T, _Enc) ->
    b(T).

conds_to_bin([], _) ->
    [];
conds_to_bin([{_, El} = Cond | L], Enc) when is_list(El) ->
    [[$(, cond_to_bin(Cond, Enc), $)] | conds_to_bin(L, Enc)];
conds_to_bin([E | L], Enc) ->
    [cond_to_bin(E, Enc) | conds_to_bin(L, Enc)].

cond_to_bin({E}, _Enc) when is_binary(E) ->
    E;
cond_to_bin(L, Enc) when is_list(L) ->
    cond_to_bin({'and', L}, Enc);
cond_to_bin({'and', L}, Enc) ->
    sp(intersperse(<<"AND">>, conds_to_bin(L, Enc)));
cond_to_bin({'or', L}, Enc) ->
    sp(intersperse(<<"OR">>, conds_to_bin(L, Enc)));
cond_to_bin({'exists', C}, Enc) ->
    BinC = case C of
               #{query := select} -> val_to_bin(C, Enc)
           end,
    sp([<<"EXISTS">>, BinC]);
cond_to_bin({'not', C}, Enc) ->
    BinC0 = cond_to_bin(C, Enc),
    BinC = case C of
               {_Op, L} when is_list(L) -> [$(, BinC0, $)];
               L when is_list(L) -> [$(, BinC0, $)];
               _ -> BinC0
           end,
    sp([<<"NOT">>, BinC]);
cond_to_bin({Col, 'between', Value0, 'and', Value1}, Enc) ->
    sp([col_to_bin(Col, Enc), <<"BETWEEN">>,
        val_to_bin(Value0, Enc), <<"AND">>,
        val_to_bin(Value1, Enc)]);
cond_to_bin({Col, Op, Value}, Enc) ->
    case lists:member(Op, ['<', '>', '<=', '>=', '=', '<>', '!=']) of
        true ->
            [col_to_bin(Col, Enc), b(Op), val_to_bin(Value, Enc)];
        false ->
            BOp = iolist_to_binary(string:to_upper(binary_to_list(b(Op)))),
            sp([col_to_bin(Col, Enc), BOp, val_to_bin(Value, Enc)])
    end.

col_to_bin({Table, Col}, _Enc) ->
    b({[b(Table), $., b(Col)]});
col_to_bin({B}, _Enc) when is_binary(B) ->
    B;
col_to_bin({L}, _Enc) when is_list(L) ->
    iolist_to_binary(L);
col_to_bin(C, Enc) ->
    Enc(C).

cols_to_bin(Cols, Enc) ->
    cm(lists:map(
         fun ({Col, 'as', Alias}) ->
                 sp([col_to_bin(Col, Enc), <<"AS">>, b(Alias)]);
             ('*') ->
                 <<"*">>;
             (C) ->
                 col_to_bin(C, Enc)
         end,
         Cols
        )).

val_to_bin(#{query := select} = Q, Enc) ->
    [$(, to_iolist(Q, Enc), $)];
val_to_bin(null, _Enc) ->
    <<"NULL">>;
val_to_bin({V0, Op, V1}, Enc) ->
    BV0 = val_to_bin(V0, Enc),
    BV1 = val_to_bin(V1, Enc),
    [BV0, b(Op), BV1];
val_to_bin(V, Enc) ->
    col_to_bin(V, Enc).

select_to_bin(_, undefined, _Enc) ->
    [];
select_to_bin(from, {undefined, _}, _Enc) ->
    [];
select_to_bin(from, {Table, Joins}, Enc) ->
    TableBin = table_to_bin(Table, Enc),
    [sp([<<"FROM">>, TableBin] ++ joins_to_bin(Joins, Enc))];
select_to_bin(where, Cond, Enc) ->
    [sp([<<"WHERE">>, cond_to_bin(Cond, Enc)])];
select_to_bin(limit, Limit, _Enc) ->
    [sp([<<"LIMIT">>, b(Limit)])];
select_to_bin(offset, Offset, _Enc) ->
    [sp([<<"OFFSET">>, b(Offset)])];
select_to_bin(order_by, Order, Enc) ->
    [sp([<<"ORDER BY">>, order_to_bin(Order, Enc)])].

insert_to_bin(_, undefined, _Enc) ->
    [];
insert_to_bin(into, Into, Enc) ->
    IntoBin = table_to_bin(Into, Enc),
    [sp([<<"INTO">>, IntoBin])];
insert_to_bin(cols, Columns, _Enc) ->
    [[$(, cm([b(C) || C <- Columns]), $)]];
insert_to_bin(vals, default_values, _Enc) ->
    [<<"DEFAULT VALUES">>];
insert_to_bin(vals, Values0, Enc) ->
    Values = lists:map(
               fun (#{query := select} = Q) -> [$(, to_iolist(Q, Enc), $)];
                   (Vs) -> [$(, cm(lists:map(fun (default) -> <<"DEFAULT">>;
                                                 (V) -> val_to_bin(V, Enc) end, Vs)), $)] end,
               Values0),
    [sp([<<"VALUES">>, cm(Values)])];
insert_to_bin(on_conflict, OnConf, Enc) ->
    [sp([<<"ON CONFLICT">>, on_conflict_to_bin(OnConf, Enc)])];
insert_to_bin(returning, Returning, Enc) ->
    [sp([<<"RETURNING">>, cols_to_bin(Returning, Enc)])].

delete_to_bin(_, undefined, _Enc) ->
    [];
delete_to_bin(where, Cond, Enc) ->
    [sp([<<"WHERE">>, cond_to_bin(Cond, Enc)])];
delete_to_bin(returning, Returning, Enc) ->
    [sp([<<"RETURNING">>, cols_to_bin(Returning, Enc)])].

update_to_bin(_, undefined, _Enc) ->
    [];
update_to_bin(set, Set, Enc) ->
    [sp([<<"SET">>, set_to_bin(Set, Enc)])];
update_to_bin(where, Cond, Enc) ->
    [sp([<<"WHERE">>, cond_to_bin(Cond, Enc)])];
update_to_bin(returning, Returning, Enc) ->
    [sp([<<"RETURNING">>, cols_to_bin(Returning, Enc)])].

joins_to_bin(NotList, Enc) when not is_list(NotList) ->
    joins_to_bin([NotList], Enc);
joins_to_bin([undefined | Rest], Enc) ->
    joins_to_bin(Rest, Enc);
joins_to_bin([Join | Rest], Enc) ->
    [to_iolist(Join, Enc) | joins_to_bin(Rest, Enc)];
joins_to_bin([], _)->
    [].

order_to_bin(L, Enc) when is_list(L) ->
    [$(, cm([order_to_bin(E, Enc) || E <- L]), $)];
order_to_bin({O, AscDesc}, Enc) when AscDesc =:= asc; AscDesc =:= desc ->
    sp([order_to_bin(O, Enc), case AscDesc of
                                  asc -> <<"ASC">>;
                                  desc -> <<"DESC">>
                              end]);
order_to_bin(I, _Enc) when is_integer(I) ->
    b(I);
order_to_bin(A, Enc) when is_atom(A); is_tuple(A) ->
    val_to_bin(A, Enc).

set_to_bin(Set, Enc) ->
    cm(lists:map(
         fun ({Col, Val}) ->
                 ValBin = case Val of
                              default -> <<"DEFAULT">>;
                              _ -> val_to_bin(Val, Enc)
                          end,
                 [col_to_bin(Col, Enc), $=, ValBin]
         end,
         Set)).

on_conflict_to_bin(do_nothing, _Enc) ->
    [<<"DO NOTHING">>];
on_conflict_to_bin({do_update, Update}, Enc) ->
    UpdateBin = set_to_bin(Update, Enc),
    sp([<<"DO UPDATE SET">>, UpdateBin]).

alter_query(F, How, Q) when is_function(How, 1) ->
    alter_query(F, How(maps:get(F, Q)), Q);
alter_query(F, NewValue, Q) ->
    maps:put(F, NewValue, Q).

b(L) when is_list(L) ->
    [b(E) || E <- L];
b(A) when is_atom(A) ->
    atom_to_binary(A, utf8);
b(I) when is_integer(I) ->
    integer_to_binary(I);
b({L}) when is_list(L) ->
    L;
b({B}) when is_binary(B) ->
    B;
b(B) when is_binary(B) ->
    B.

cm(L) ->
    intersperse($,, L).

sp(L) ->
    intersperse($\ , L).

intersperse(_, []) ->
    [];
intersperse(Ch, L) ->
    tl(lists:append([[Ch, E] || E <- L])).

opts(Opts) when is_list(Opts) ->
    maps:from_list(proplists:unfold(Opts));
opts(Opts) when is_map(Opts) ->
    Opts.
