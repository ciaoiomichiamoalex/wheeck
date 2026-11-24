DROP VIEW IF EXISTS wheeck.vw_gap_message;
CREATE OR REPLACE VIEW wheeck.vw_gap_message AS
SELECT id,
    SUBSTRING(
        message_text,
        STRPOS(message_text, 'doc number ') + LENGTH('doc number '),
        STRPOS(message_text, ' of year ') - (STRPOS(message_text, 'doc number ') + LENGTH('doc number '))
    )::INTEGER AS document_number,
    SUBSTRING(
        message_text,
        STRPOS(message_text, ' of year ') + LENGTH(' of year '),
        LENGTH(message_text)
    )::INTEGER AS document_year
FROM wheeck.delivery_warning
WHERE message_genre = 'GAP'
    AND status IS TRUE
;
