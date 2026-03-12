DROP TABLE IF EXISTS public.repair_log_tools;
DROP TABLE IF EXISTS public.repair_logs;
DROP TABLE IF EXISTS public.repair_records;
DROP TABLE IF EXISTS public.parts;
DROP TABLE IF EXISTS public.items;
DROP TABLE IF EXISTS public.resources;
DROP TABLE IF EXISTS public.doc_units;
DROP TABLE IF EXISTS public.users;

DROP SEQUENCE IF EXISTS public.items_id_seq;
DROP SEQUENCE IF EXISTS public.parts_id_seq;
DROP SEQUENCE IF EXISTS public.repair_logs_id_seq;
DROP SEQUENCE IF EXISTS public.repair_records_id_seq;
DROP SEQUENCE IF EXISTS public.resources_id_seq;
DROP SEQUENCE IF EXISTS public.doc_units_id_seq;
DROP SEQUENCE IF EXISTS public.users_id_seq;

DROP FUNCTION IF EXISTS public.trg_set_last_update();
DROP TYPE IF EXISTS public.resource_status;
