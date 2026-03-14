CREATE TRIGGER set_last_update_after_log
AFTER INSERT ON public.repair_logs
FOR EACH ROW
EXECUTE FUNCTION public.trg_set_last_update();

CREATE TRIGGER resources_changed_notify
AFTER INSERT OR UPDATE OR DELETE ON public.resources
FOR EACH ROW
EXECUTE FUNCTION public.notify_resources_changed();
