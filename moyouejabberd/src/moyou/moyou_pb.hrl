-record(bound, {type, size}).
-record(login, {server, user, password, resource, resv1, resv2, resv3}).
-record(offlinemsg, {status, jid, msg, resv1, resv2, resv3}).
-record(msg, {msg_id, server_id, from, to_user, msg_type, group_id, msg_time, serial_num, number, body, payload, resv1, resv2, resv3}).
-record(heart, {time}).
