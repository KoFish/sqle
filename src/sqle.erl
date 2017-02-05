-module(sqle).

%% API exports
-export([select/1, select/2, select/3]).
-export([insert/2, insert/3]).
-export([delete/1, delete/2]).
-export([update/2, update/3]).
-export([join/1, join/2]).
-export([alter/3]).
-export([to_iolist/2]).
-export([to_binary/2]).

%% Query object definitions
-record(select, {
          cols,
          table,
          joins,
          where,
          limit,
          offset,
          order_by
         }
       ).
-record(insert, {
          into,
          cols,
          vals,
          on_conflict,
          returning
         }
       ).
-record(update, {
          table,
          vals,
          where,
          returning
         }
       ).
-record(delete, {
          from,
          where,
          returning
         }
       ).

-record(join, {
          dir,
          natural = false,
          using = [],
          table,
          on
         }
       ).

%%====================================================================
%% API functions
%%====================================================================

%% SELECT -----------------------------------------------------------------------------------------
select(Cols) ->
    #select{cols = Cols}.

select(Cols, From) ->
    set_table(select(Cols), From).

select(Cols, From, Opts) ->
    Q = select(Cols, From),
    maps:fold(fun set_opt/3, Q, opts(Opts)).

%% INSERT -----------------------------------------------------------------------------------------
insert(Into, Values) ->
    #insert{into = Into,
            vals = Values}.

insert(Into, Values, Opts) ->
    Q = insert(Into, Values),
    maps:fold(fun set_opt/3, Q, opts(Opts)).

%% DELETE------------------------------------------------------------------------------------------
delete(From) ->
    #delete{from = From}.

delete(From, Opts) ->
    Q = delete(From),
    maps:fold(fun set_opt/3, Q, opts(Opts)).

%% UPDATE -----------------------------------------------------------------------------------------
update(Table, Set) ->
    #update{table = Table, vals = Set}.

update(Table, Set, Opts) ->
    Q = update(Table, Set),
    maps:fold(fun set_opt/3, Q, opts(Opts)).

%% JOIN -------------------------------------------------------------------------------------------
join(Table) ->
    #join{table = Table}.

join(Table, Opts) ->
    J = join(Table),
    maps:fold(fun set_opt/3, J, opts(Opts)).

%% ALTER operation --------------------------------------------------------------------------------
alter(What, How, Q) ->
    Record = element(1, Q),
    Fields0 = case Record of
                  select -> record_info(fields, select);
                  update -> record_info(fields, update);
                  delete -> record_info(fields, delete);
                  insert -> record_info(fields, insert)
              end,
    EnumFields = lists:zip(lists:seq(2, length(Fields0) + 1), Fields0),
    case lists:keyfind(What, 2, EnumFields) of
        {N, What} -> {ok, alter_query(Record, N, How, Q)};
        false -> {error, {invalid_field, What}}
    end.

%% SELECT -----------------------------------------------------------------------------------------
to_iolist(#select{cols = Cols} = Q, Enc) ->
    sp([<<"SELECT">>, cols_to_bin(Cols, Enc)]
       ++ [select_to_bin(from, {Q#select.table, Q#select.joins}, Enc) || Q#select.table =/= undefined]
       ++ [select_to_bin(where, Q#select.where, Enc) || Q#select.where =/= undefined]
       ++ [select_to_bin(limit, Q#select.limit, Enc) || Q#select.limit =/= undefined]
       ++ [select_to_bin(offset, Q#select.offset, Enc) || Q#select.offset =/= undefined]
       ++ [select_to_bin(order_by, Q#select.order_by, Enc) || Q#select.order_by =/= undefined]);
%% INSERT -----------------------------------------------------------------------------------------
to_iolist(#insert{into = Into, vals = Vals} = Q, Enc) ->
    sp([<<"INSERT">>]
       ++ [insert_to_bin(into, Into, Enc)]
       ++ [insert_to_bin(cols, Q#insert.cols, Enc) || Q#insert.cols =/= undefined]
       ++ [insert_to_bin(vals, Vals, Enc)]
       ++ [insert_to_bin(on_conflict, Q#insert.on_conflict, Enc) || Q#insert.on_conflict =/= undefined]
       ++ [insert_to_bin(returning, Q#insert.returning, Enc) || Q#insert.returning =/= undefined]
      );
%% DELETE -----------------------------------------------------------------------------------------
to_iolist(#delete{from = Table} = Q, Enc) ->
    sp([<<"DELETE">>, <<"FROM">>, table_to_bin(Table, Enc)]
       ++ [delete_to_bin(where, Q#delete.where, Enc) || Q#delete.where =/= undefined]
       ++ [delete_to_bin(returning, Q#delete.returning, Enc) || Q#delete.returning =/= undefined]
      );
%% UPDATE -----------------------------------------------------------------------------------------
to_iolist(#update{table = Table, vals = Set} = Q, Enc) ->
    sp([<<"UPDATE">>, table_to_bin(Table, Enc)]
       ++ [update_to_bin(set, Set, Enc)]
       ++ [update_to_bin(where, Q#update.where, Enc) || Q#update.where =/= undefined]
       ++ [update_to_bin(returning, Q#update.returning, Enc) || Q#update.returning =/= undefined]
      );
%% JOIN -------------------------------------------------------------------------------------------
to_iolist(#join{table = Table} = J, Enc) ->
    sp(
      lists:append(
        [[<<"NATURAL">>] || J#join.natural =:= true]
        ++ [[<<"INNER">>] || J#join.dir =:= inner]
        ++ [[<<"LEFT">>] || element(1, J#join.dir) =:= left]
        ++ [[<<"RIGHT">>] || element(1, J#join.dir) =:= right]
        ++ [[<<"FULL">>] || element(1, J#join.dir) =:= full]
        ++ [[<<"OUTER">>] || element(2, J#join.dir) =:= outer]
        ++ [[<<"JOIN">>, table_to_bin(Table, Enc)]]
        ++ [[<<"USING">>, [$(, cm([b(U) || U <- J#join.using]), $)]] || J#join.using =/= []]
        ++ [[<<"ON">>, cond_to_bin(J#join.on, Enc)] || J#join.on =/= undefined]
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

set_table(#select{} = Q, From) ->
    Q#select{table = From}.

%% SELECT -----------------------------------------------------------------------------------------
set_opt(where, Where, #select{} = Q) ->
    Q#select{where = Where};
set_opt(limit, Limit, #select{} = Q) ->
    Q#select{limit = Limit};
set_opt(offset, Offset, #select{} = Q) ->
    Q#select{offset = Offset};
set_opt(order_by, Order, #select{} = Q) ->
    Q#select{order_by = Order};
set_opt(joins, Joins, #select{} = Q) ->
    Q#select{joins = lists:map(
                       fun ({Table, Opts}) ->
                               join(Table, Opts);
                           (Table) ->
                               join(Table)
                       end,
                       Joins)};
%% INSERT -----------------------------------------------------------------------------------------
set_opt(columns, Columns, #insert{} = Q) ->
    Q#insert{cols = Columns};
set_opt(on_conflict, OnConf, #insert{} = Q) ->
    Q#insert{on_conflict = OnConf};
set_opt(returning, Returning, #insert{} = Q) ->
    Q#insert{returning = Returning};
%% DELETE -----------------------------------------------------------------------------------------
set_opt(where, Where, #delete{} = Q) ->
    Q#delete{where = Where};
set_opt(returning, Returning, #delete{} = Q) ->
    Q#delete{returning = Returning};
%% UPDATE -----------------------------------------------------------------------------------------
set_opt(where, Where, #update{} = Q) ->
    Q#update{where = Where};
set_opt(returning, Returning, #update{} = Q) ->
    Q#update{returning = Returning};
%% JOIN -------------------------------------------------------------------------------------------
set_opt(inner, true, #join{} = J) ->
    J#join{dir = inner};
set_opt(outer, true, #join{dir = {_, outer}} = J) ->
    J;
set_opt(outer, true, #join{} = J) ->
    J#join{dir = {undefined, outer}};
set_opt(left, true, #join{} = J) ->
    J#join{dir = {left, outer}};
set_opt(right, true, #join{} = J) ->
    J#join{dir = {right, outer}};
set_opt(full, true, #join{} = J) ->
    J#join{dir = {full, outer}};
set_opt(natural, Nat, #join{} = J) ->
    J#join{natural = Nat};
set_opt(using, Using, #join{} = J) ->
    J#join{using = Using};
set_opt(on, Cond, #join{} = J) ->
    J#join{on = Cond}.

table_to_bin({T, 'as', A}, Enc) ->
    sp([table_to_bin(T, Enc), <<"AS">>, b(A)]);
table_to_bin(Q, Enc) when is_record(Q, select) ->
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
    BinC = if is_list(C) -> [$(, cm([val_to_bin(E, Enc) || E <- C]), $)];
              is_record(C, select) -> val_to_bin(C, Enc)
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

val_to_bin(Q, Enc) when is_record(Q, select) ->
    [$(, to_iolist(Q, Enc), $)];
val_to_bin(null, _Enc) ->
    <<"NULL">>;
val_to_bin({V0, Op, V1}, Enc) ->
    BV0 = val_to_bin(V0, Enc),
    BV1 = val_to_bin(V1, Enc),
    [BV0, b(Op), BV1];
val_to_bin(V, Enc) ->
    col_to_bin(V, Enc).

select_to_bin(from, {Table, Joins}, Enc) ->
    TableBin = table_to_bin(Table, Enc),
    sp([<<"FROM">>, TableBin] ++ joins_to_bin(Joins, Enc));
select_to_bin(where, Cond, Enc) ->
    sp([<<"WHERE">>, cond_to_bin(Cond, Enc)]);
select_to_bin(limit, Limit, _Enc) ->
    sp([<<"LIMIT">>, b(Limit)]);
select_to_bin(offset, Offset, _Enc) ->
    sp([<<"OFFSET">>, b(Offset)]);
select_to_bin(order_by, Order, Enc) ->
    sp([<<"ORDER BY">>, order_to_bin(Order, Enc)]).

insert_to_bin(into, Into, Enc) ->
    IntoBin = table_to_bin(Into, Enc),
    sp([<<"INTO">>, IntoBin]);
insert_to_bin(cols, Columns, _Enc) ->
    [$(, cm([b(C) || C <- Columns]), $)];
insert_to_bin(vals, default_values, _Enc) ->
    <<"DEFAULT VALUES">>;
insert_to_bin(vals, Values0, Enc) ->
    Values = lists:map(
               fun (Q) when is_record(Q, select) -> [$(, to_iolist(Q, Enc), $)];
                   (Vs) -> [$(, cm(lists:map(fun (default) -> <<"DEFAULT">>;
                                                 (V) -> val_to_bin(V, Enc) end, Vs)), $)] end,
               Values0),
    sp([<<"VALUES">>, cm(Values)]);
insert_to_bin(on_conflict, OnConf, Enc) ->
    sp([<<"ON CONFLICT">>, on_conflict_to_bin(OnConf, Enc)]);
insert_to_bin(returning, Returning, Enc) ->
    sp([<<"RETURNING">>, cols_to_bin(Returning, Enc)]).

delete_to_bin(where, Cond, Enc) ->
    sp([<<"WHERE">>, cond_to_bin(Cond, Enc)]);
delete_to_bin(returning, Returning, Enc) ->
    sp([<<"RETURNING">>, cols_to_bin(Returning, Enc)]).

update_to_bin(set, Set, Enc) ->
    sp([<<"SET">>, set_to_bin(Set, Enc)]);
update_to_bin(where, Cond, Enc) ->
    sp([<<"WHERE">>, cond_to_bin(Cond, Enc)]);
update_to_bin(returning, Returning, Enc) ->
    sp([<<"RETURNING">>, cols_to_bin(Returning, Enc)]).

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
    <<"DO NOTHING">>;
on_conflict_to_bin({do_update, Update}, Enc) ->
    UpdateBin = set_to_bin(Update, Enc),
    sp([<<"DO UPDATE SET">>, UpdateBin]).

alter_query(RecordName, F, How, Q) when is_function(How, 1) ->
    alter_query(RecordName, F(element(F, Q)), How, Q);
alter_query(_RecordName, F, NewValue, Q) ->
    setelement(F, Q, NewValue).

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
