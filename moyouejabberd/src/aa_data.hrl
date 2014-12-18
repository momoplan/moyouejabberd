
-record(user_msg, {id, from, to, packat, timestamp, expire_time,score}).


-record(user_msg_list, {id, msg_list}).

-define(GOUPR_MEMBER_TABLE, group_members).
-record(group_members, {gid, members = []}).

-define(MY_USER_TABLES, my_user_table_info).

-record(?MY_USER_TABLES, {id, msg_table, msg_list_table}).


-define(MY_USER_MSGPID_INFO, my_user_msgpid_info).

-record(?MY_USER_MSGPID_INFO, {user, pid}).


-define(DB, my_db_conn).

