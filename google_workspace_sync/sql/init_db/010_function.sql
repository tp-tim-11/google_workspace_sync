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

CREATE FUNCTION public.notify_resources_changed()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF current_setting('gws.sync_origin', true) = 'sheet_pull' THEN
    RETURN COALESCE(NEW, OLD);
  END IF;

  PERFORM pg_notify(
    'resources_changed',
    json_build_object(
      'operation',
      TG_OP,
      'name',
      COALESCE(NEW.name, OLD.name)
    )::text
  );

  RETURN COALESCE(NEW, OLD);
END;
$$;
