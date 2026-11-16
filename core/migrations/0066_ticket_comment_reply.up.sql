-- 0066 工单评论：回复（一层）。
ALTER TABLE ticket_comment ADD COLUMN parent_id uuid REFERENCES ticket_comment (id);
COMMENT ON COLUMN ticket_comment.parent_id IS '所回复的评论；空 = 顶层评论。回复只有一层：被回复的一定是顶层评论';
