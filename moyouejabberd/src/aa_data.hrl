
-record(user_msg, {id, from, to, packat, timestamp, expire_time,score}).


-record(user_msg_list, {id, msg_list}).

-define(GOUPR_MEMBER_TABLE, group_members).
-record(group_members, {gid, members = []}).