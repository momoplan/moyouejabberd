%%%--------------------------------------
%%% @Module  : db_sql
%%% @Author  : xyao
%%% @Email   : jiexiaowen@gmail.com
%%% @Created : 2010.05.10
%%% @Description: MYSQL数据库操作 
%%%--------------------------------------
-module(db_sql).
-compile(export_all).
-include("aa_data.hrl").
-include("emysql.hrl").

%% 执行一个SQL查询,返回影响的行数
execute(Sql) ->
	db:execute(?DB, Sql).

execute(Sql, Args) ->
	db:execute(?DB, Sql, Args).

%% 执行插入语句返回插入ID
insert(Sql) ->
    db:insert(?DB, Sql).
insert(Sql, Args) ->
    db:insert(?DB, Sql, Args).

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
select_limit(Sql, Offset, Num) ->
    db:select_limit(?DB,Sql, Offset, Num).
select_limit(Sql, Args, Offset, Num) ->
    db:select_limit(?DB,Sql, Args, Offset, Num).

%% 取出查询结果中的第一行第一列
%% 未找到时返回null
get_one(Sql) ->
    db:get_one(?DB, Sql).
get_one(Sql, Args) ->
    db:get_one(?DB, Sql, Args).

%% 取出查询结果中的第一行
get_row(Sql) ->
    db:get_row(?DB, Sql).
get_row(Sql, Args) ->
    db:get_row(?DB, Sql, Args).

%% 取出查询结果中的所有行
get_all(Sql) ->
    db:get_all(?DB, Sql).
get_all(Sql, Args) ->
    db:get_all(?DB, Sql, Args).