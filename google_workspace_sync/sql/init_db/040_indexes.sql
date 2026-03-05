CREATE UNIQUE INDEX items_name_code_key
ON public.items USING btree (name, code);

CREATE INDEX idx_items_name
ON public.items USING btree (name);

CREATE UNIQUE INDEX parts_item_id_name_key
ON public.parts USING btree (item_id, name);

CREATE INDEX idx_parts_item_name
ON public.parts USING btree (item_id, name);

CREATE INDEX idx_repair_log_tools_tool_damaged
ON public.repair_log_tools USING btree (tool_id, is_damaged);

CREATE INDEX idx_repair_logs_record_dt
ON public.repair_logs USING btree (record_id, dt DESC);

CREATE INDEX idx_repair_logs_part
ON public.repair_logs USING btree (part_id);

CREATE UNIQUE INDEX repair_records_user_id_item_id_key
ON public.repair_records USING btree (user_id, item_id);

CREATE INDEX idx_repair_records_last_update
ON public.repair_records USING btree (last_update DESC);

CREATE UNIQUE INDEX resources_nazov_unique
ON public.resources USING btree (nazov);

CREATE INDEX resources_not_deleted_idx
ON public.resources USING btree (nazov)
WHERE (deleted = false);

CREATE UNIQUE INDEX users_email_key
ON public.users USING btree (email);

CREATE INDEX idx_users_last_first
ON public.users USING btree (last_name, first_name);
