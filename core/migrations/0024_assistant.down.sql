-- 0024_assistant.down.sql 回滚 0024：删除智能助手三表（索引随表删除）。

DROP TABLE IF EXISTS assistant_tool_call;
DROP TABLE IF EXISTS assistant_message;
DROP TABLE IF EXISTS assistant_conversation;
