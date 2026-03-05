ALTER TABLE ONLY public.parts
ADD CONSTRAINT parts_item_id_fkey
FOREIGN KEY (item_id) REFERENCES public.items(id)
ON DELETE CASCADE
NOT DEFERRABLE;

ALTER TABLE ONLY public.repair_log_tools
ADD CONSTRAINT repair_log_tools_log_id_fkey
FOREIGN KEY (log_id) REFERENCES public.repair_logs(id)
ON DELETE CASCADE
NOT DEFERRABLE;

ALTER TABLE ONLY public.repair_log_tools
ADD CONSTRAINT repair_log_tools_tool_id_fkey
FOREIGN KEY (tool_id) REFERENCES public.resources(id)
NOT DEFERRABLE;

ALTER TABLE ONLY public.repair_logs
ADD CONSTRAINT repair_logs_part_id_fkey
FOREIGN KEY (part_id) REFERENCES public.parts(id)
NOT DEFERRABLE;

ALTER TABLE ONLY public.repair_logs
ADD CONSTRAINT repair_logs_record_id_fkey
FOREIGN KEY (record_id) REFERENCES public.repair_records(id)
ON DELETE CASCADE
NOT DEFERRABLE;

ALTER TABLE ONLY public.repair_records
ADD CONSTRAINT repair_records_item_id_fkey
FOREIGN KEY (item_id) REFERENCES public.items(id)
NOT DEFERRABLE;

ALTER TABLE ONLY public.repair_records
ADD CONSTRAINT repair_records_user_id_fkey
FOREIGN KEY (user_id) REFERENCES public.users(id)
NOT DEFERRABLE;
