%%%--------------------------------------
%%% @Module  : db_sql
%%% @Author  : xyao
%%% @Email   : jiexiaowen@gmail.com
%%% @Created : 2010.05.10
%%% @Description: MYSQL数据库操作 
%%%--------------------------------------
-module(db).
-compile(export_all).
-include("emysql.hrl").

%% 执行一个SQL查询,返回影响的行数
execute(PoolId, Sql) ->
    case emysql:execute(PoolId, Sql) of
		#ok_packet{affected_rows = R} ->
			R;
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
	end.
execute(PoolId, Sql, Args) when is_atom(Sql) ->
    case emysql:execute(PoolId, Sql, Args) of
        #ok_packet{affected_rows = R} ->
			R;
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
	end;
execute(PoolId, Sql, Args) ->
    emysql:prepare(execute, Sql),
    case emysql:execute(PoolId, execute, Args) of
        #ok_packet{affected_rows = R} ->
			R;
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
	end.

%% 执行插入语句返回插入ID
insert(PoolId, Sql) ->
    case emysql:execute(PoolId, Sql) of
		#ok_packet{insert_id = R} ->
			{ok, R};
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
    end.
insert(PoolId, Sql, Args) when is_atom(Sql) ->
    case emysql:execute(PoolId, Sql, Args) of
        #ok_packet{insert_id = R} ->
			{ok, R};
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
    end;
insert(PoolId, Sql, Args) ->
    emysql:prepare(insert, Sql),
    case emysql:execute(PoolId, insert, Args) of
        #ok_packet{insert_id = R} ->
			{ok, R};
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
    end.

%% 事务处理
%% transaction(F) ->
%%     case mysql:transaction(?DB, F) of
%%         {atomic, R} -> R;
%%         {updated, {_, _, _, R, _, _}} -> R;
%%         {error, {_, _, _, _, _, Reason}} -> mysql_halt([Reason]);
%%         {aborted, {Reason, _}} -> mysql_halt([Reason]);
%%         Error -> mysql_halt([Error])
%%     end.

%% 执行分页查询返回结果中的所有行
select_limit(PoolId, Sql, Offset, Num) ->
    S = list_to_binary([Sql, <<" limit ">>, integer_to_list(Offset), <<", ">>, integer_to_list(Num)]),
    case emysql:execute(PoolId, S) of
		#result_packet{rows = R} ->
			R;
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
    end.
select_limit(PoolId, Sql, Args, Offset, Num) ->
    S = list_to_binary([Sql, <<" limit ">>, list_to_binary(integer_to_list(Offset)), <<", ">>, list_to_binary(integer_to_list(Num))]),
    emysql:prepare(select_limit, S),
    case emysql:execute(PoolId, select_limit, Args) of
        #result_packet{rows = R} ->
			R;
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
    end.

%% 取出查询结果中的第一行第一列
%% 未找到时返回null
get_one(PoolId, Sql) ->
    case emysql:execute(PoolId, Sql) of
		#result_packet{rows = []} ->
			null;
		#result_packet{rows = [[R]]} ->
			R;
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
    end.
get_one(PoolId, Sql, Args) when is_atom(Sql) ->
    case emysql:execute(PoolId, Sql, Args) of
        #result_packet{rows = []} ->
			null;
		#result_packet{rows = [[R]]} ->
			R;
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
    end;
get_one(PoolId, Sql, Args) ->
    emysql:prepare(get_one, Sql),
    case emysql:execute(PoolId, get_one, Args) of
        #result_packet{rows = []} ->
			null;
		#result_packet{rows = [[R]]} ->
			R;
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
    end.

%% 取出查询结果中的第一行
get_row(PoolId, Sql) ->
    case emysql:execute(PoolId, Sql) of
		#result_packet{rows = []} ->
			[];
		#result_packet{rows = [R|_]} ->
			R;
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
    end.
get_row(PoolId, Sql, Args) when is_atom(Sql) ->
    case emysql:execute(PoolId, Sql, Args) of
        #result_packet{rows = []} ->
			[];
		#result_packet{rows = [R|_]} ->
			R;
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
    end;
get_row(PoolId, Sql, Args) ->
    emysql:prepare(get_row, Sql),
    case emysql:execute(PoolId, get_row, Args) of
       #result_packet{rows = []} ->
			[];
		#result_packet{rows = [R|_]} ->
			R;
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
    end.

%% 取出查询结果中的所有行
get_all(PoolId, Sql) ->
    case emysql:execute(PoolId, Sql) of
		#result_packet{rows = R} ->
			R;
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
    end.
get_all(PoolId, Sql, Args) when is_atom(Sql) ->
    case emysql:execute(PoolId, Sql, Args) of
        #result_packet{rows = R} ->
			R;
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
    end;
get_all(PoolId, Sql, Args) ->
    emysql:prepare(get_all, Sql),
    case emysql:execute(PoolId, get_all, Args) of
        #result_packet{rows = R} ->
			R;
		#error_packet{code = Code, msg = Msg} ->
			mysql_halt(Sql, Code, Msg)
    end.

%% @doc 显示人可以看得懂的错误信息
mysql_halt([Sql, Reason]) ->
    erlang:error({db_error, [Sql, Reason]}).
mysql_halt(Sql, Code, Msg) ->
    erlang:error({db_error, [Sql, Code, Msg]}).


%%组合mysql insert语句
%% 使用方式make_insert_sql(test,["row","r"],["测试",123]) 相当 insert into `test` (row,r) values('测试','123')
%%Table:表名
%%Field：字段
%%Data:数据
make_insert_sql(Table, Field, Data) ->
    L = make_conn_sql(Field, Data, []),
    lists:concat(["insert into `", Table, "` set ", L]).
    
%%组合mysql insert语句
%% 使用方式make_update_sql(test,["row","r"],["测试",123],"id",1) 相当 update `test` set row='测试', r = '123' where id = '1'
%%Table:表名
%%Field：字段
%%Data:数据
%%Key:键
%%Data:值
make_update_sql(Table, Field, Data, Key, Value) ->
    L = make_conn_sql(Field, Data, []),
    lists:concat(["update `", Table, "` set ",L," where ",Key,"= '",sql_format(Value),"'"]).

make_insert_sql(Table_name, Field_Value_List) ->
%%  db:make_insert_sql(player, 
%%                         [{status, 0}, {online_flag,1}, {hp,50}, {mp,30}]).
 	{Vsql, _Count1} =
		lists:mapfoldl(
	  		fun(Field_value, Sum) ->	
				Expr = case Field_value of
						 {Field, Val} -> 
							 case is_binary(Val) orelse is_list(Val) of 
								 true -> io_lib:format("`~s`='~s'",[Field, re:replace(Val,"'","''",[global,{return,binary}])]);
							 	 _-> io_lib:format("`~s`='~p'",[Field, Val])
							 end
					end,
				S1 = if Sum == length(Field_Value_List) -> io_lib:format("~s ",[Expr]);
						true -> io_lib:format("~s,",[Expr])
					 end,
 				{S1, Sum+1}
			end,
			1, Field_Value_List),
	lists:concat(["insert into `", Table_name, "` set ",
	 			  lists:flatten(Vsql)
				 ]).

make_replace_sql(Table_name, Field_Value_List) ->
%%  db:make_replace_sql(player, 
%%                         [{status, 0}, {online_flag,1}, {hp,50}, {mp,30}]).
 	{Vsql, _Count1} =
		lists:mapfoldl(
	  		fun(Field_value, Sum) ->	
				Expr = case Field_value of
						 {Field, Val} -> 
							 case is_binary(Val) orelse is_list(Val) of 
								 true -> io_lib:format("`~s`='~s'",[Field, re:replace(Val,"'","''",[global,{return,binary}])]);
							 	 _-> io_lib:format("`~s`=~p",[Field, Val])
							 end
					end,
				S1 = if Sum == length(Field_Value_List) -> io_lib:format("~s ",[Expr]);
						true -> io_lib:format("~s,",[Expr])
					 end,
 				{S1, Sum+1}
			end,
			1, Field_Value_List),
	lists:concat(["replace into `", Table_name, "` set ",
	 			  lists:flatten(Vsql)
				 ]).

make_update_sql(Table_name, Field_Value_List, Where_List) ->
%%  db:make_update_sql(player, 
%%                         [{status, 0}, {online_flag,1}, {hp,50, add}, {mp,30,sub}],
%%                         [{id, 11}]).
 	{Vsql, _Count1} =
		lists:mapfoldl(
	  		fun(Field_value, Sum) ->	
				Expr = case Field_value of
						 {Field, Val, add} -> io_lib:format("`~s`=`~s`+~p", [Field, Field, Val]);
						 {Field, Val, sub} -> io_lib:format("`~s`=`~s`-~p", [Field, Field, Val]);						 
						 {Field, Val} -> 
							 case is_binary(Val) orelse is_list(Val) of 
								 true -> io_lib:format("`~s`='~s'",[Field, re:replace(Val,"'","''",[global,{return,binary}])]);
							 	 _-> io_lib:format("`~s`='~p'",[Field, Val])
							 end
					end,
				S1 = if Sum == length(Field_Value_List) -> io_lib:format("~s ",[Expr]);
						true -> io_lib:format("~s,",[Expr])
					 end,
 				{S1, Sum+1}
			end,
			1, Field_Value_List),
	{Wsql, Count2} = get_where_sql(Where_List),
	WhereSql = 
		if Count2 > 1 -> lists:concat(["where ", lists:flatten(Wsql)]);
	   			 true -> ""
		end,
	lists:concat(["update `", Table_name, "` set ",
	 			  lists:flatten(Vsql), WhereSql, ""
				 ]).

make_delete_sql(Table_name, Where_List) ->
%% db:make_delete_sql(player, [{id, "=", 11, "and"},{status, 0}]).
	{Wsql, Count2} = get_where_sql(Where_List),
	WhereSql = 
		if Count2 > 1 -> lists:concat(["where ", lists:flatten(Wsql)]);
	   			 true -> ""
		end,
	lists:concat(["delete from `", Table_name, "` ", WhereSql, ""]).

make_select_sql(Table_name, Fields_sql, Where_List) ->
	make_select_sql(Table_name, Fields_sql, Where_List, [], []).

make_select_sql(Table_name, Fields_sql, Where_List, Order_List, Limit_num) ->
%% db:make_select_sql(player, "*", [{status, 1}], [{id,desc},{status}],[]).
%% db:make_select_sql(player, "id, status", [{id, 11}], [{id,desc},{status}],[]).
	{Wsql, Count1} = get_where_sql(Where_List),
	WhereSql = 
		if Count1 > 1 -> lists:concat(["where ", lists:flatten(Wsql)]);
	   			 true -> ""
		end,
	{Osql, Count2} = get_order_sql(Order_List),
	OrderSql = 
		if Count2 > 1 -> lists:concat(["order by ", lists:flatten(Osql)]);
	   			 true -> ""
		end,
	LimitSql = case Limit_num of
				   [] -> "";
				   [Num] -> lists:concat(["limit ", Num])
			   end,
	lists:concat(["select ", Fields_sql," from `", Table_name, "` ", WhereSql, OrderSql, LimitSql]).


get_order_sql(Order_List) ->
%%  排序用列表方式：[{id, desc},{status}]
	lists:mapfoldl(
  		fun(Field_Order, Sum) ->	
			Expr = 
				case Field_Order of   
					{Field, Order} ->
							io_lib:format("~p ~p",[Field, Order]);
					{Field} ->
							io_lib:format("~p",[Field]);
					 _-> ""
				   end,
			S1 = if Sum == length(Order_List) -> io_lib:format("~s ",[Expr]);
					true ->	io_lib:format("~s,",[Expr])
				 end,
			{S1, Sum+1}
		end,
		1, Order_List).

get_where_sql(Where_List) ->
%%  条件用列表方式：[{},{},{}]
%%  每一个条件形式(一共三种)：
%%		1、{idA, "<>", 10, "or"}   	<===> {字段名, 操作符, 值，下一个条件的连接符}
%% 	    2、{idB, ">", 20}   			<===> {idB, ">", 20，"and"}
%% 	    3、{idB, 20}   				<===> {idB, "=", 20，"and"}		
	lists:mapfoldl(
  		fun(Field_Operator_Val, Sum) ->	
			[Expr, Or_And_1] = 
				case Field_Operator_Val of   
					{Field, Operator, Val, Or_And} ->
						case is_binary(Val) orelse is_list(Val) of 
						 	true -> [io_lib:format("`~s`~s'~s'",[Field, Operator, re:replace(Val,"'","''",[global,{return,binary}])]), Or_And];
							_-> [io_lib:format("`~s`~s'~p'",[Field, Operator, Val]), Or_And]
						end;
					{Field, Operator, Val} ->
						case is_binary(Val) orelse is_list(Val) of 
						 	true -> [io_lib:format("`~s`~s'~s'",[Field, Operator, re:replace(Val,"'","''",[global,{return,binary}])]), "and"];
							_-> [io_lib:format("`~s`~s'~p'",[Field, Operator, Val]),"and"]
						end;
					{Field, Val} ->  
						case is_binary(Val) orelse is_list(Val) of 
						 	true -> [io_lib:format("`~s`='~s'",[Field, re:replace(Val,"'","''",[global,{return,binary}])]), "and"];
							_-> [io_lib:format("`~s`='~p'",[Field, Val]), "and"]
						end;
					 _-> ""
				   end,
			S1 = if Sum == length(Where_List) -> io_lib:format("~s ",[Expr]);
					true ->	io_lib:format("~s ~s ",[Expr, Or_And_1])
				 end,
			{S1, Sum+1}
		end,
		1, Where_List).
    
make_conn_sql([], _, L ) ->
    L ;
make_conn_sql(_, [], L ) ->
    L ;
make_conn_sql([F | T1], [D | T2], []) ->
    L  = [F," = '",sql_format(D),"'"],
    make_conn_sql(T1, T2, L);
make_conn_sql([F | T1], [D | T2], L) ->
    L1  = L ++ [",", F," = '",sql_format(D),"'"],
    make_conn_sql(T1, T2, L1).

sql_format(S) when is_integer(S)->
    integer_to_list(S);
sql_format(S) when is_float(S)->
    float_to_list(S);
sql_format(S) ->
    S.
