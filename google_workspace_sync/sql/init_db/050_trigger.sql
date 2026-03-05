CREATE TRIGGER set_last_update_after_log
AFTER INSERT ON public.repair_logs
FOR EACH ROW
EXECUTE FUNCTION public.trg_set_last_update();
