DROP TRIGGER IF EXISTS trg_sync_discard_status ON public.delivery_warning;
CREATE TRIGGER trg_sync_discard_status
AFTER UPDATE OF status ON public.delivery_warning
FOR EACH ROW
WHEN (NEW.message_genre = 'DISCARD')
EXECUTE FUNCTION public.sync_discard_status()
;
