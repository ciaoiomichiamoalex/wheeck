DROP FUNCTION IF EXISTS public.sync_discard_status;
CREATE OR REPLACE FUNCTION public.sync_discard_status()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE public.delivery_discard
    SET status = NEW.status
    WHERE id_warning_message = NEW.id
    ;

    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL
;
