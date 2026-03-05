CREATE FUNCTION public.trg_set_last_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE public.repair_records
    SET last_update = NEW.dt
  WHERE id = NEW.record_id
    AND last_update < NEW.dt;
  RETURN NEW;
END;
$$;
