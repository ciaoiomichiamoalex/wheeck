DROP FUNCTION IF EXISTS wheeck.sync_discard_status;
CREATE OR REPLACE FUNCTION wheeck.sync_discard_status()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE wheeck.delivery_discard
    SET status = NEW.status
    WHERE id_warning_message = NEW.id
    ;

    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL
;
