DROP VIEW IF EXISTS public.vw_discard_message;
CREATE OR REPLACE VIEW public.vw_discard_message AS
SELECT id,
    NULLIF(SUBSTRING(
        message_text,
        STRPOS(message_text, '[number: ') + LENGTH('[number: '),
        STRPOS(message_text, ', genre: ') - (STRPOS(message_text, '[number: ') + LENGTH('[number: '))
    ), 'None')::INTEGER AS document_number,
    NULLIF(SUBSTRING(
        message_text,
        STRPOS(message_text, ', genre: ') + LENGTH(', genre: '),
        STRPOS(message_text, ', date: ') - (STRPOS(message_text, ', genre: ') + LENGTH(', genre: '))
    ), 'None') AS document_genre,
    NULLIF(SUBSTRING(
        message_text,
        STRPOS(message_text, ', date: ') + LENGTH(', date: '),
        STRPOS(message_text, ']') - (STRPOS(message_text, ', date: ') + LENGTH(', date: '))
    ), 'None')::DATE AS document_date,
    SUBSTRING(
        message_text,
        STRPOS(message_text, 'for error on ') + LENGTH('for error on '),
        STRPOS(message_text, '[number: ') - (STRPOS(message_text, 'for error on ') + LENGTH('for error on '))
    ) AS error,
    SUBSTRING(
        message_text,
        STRPOS(message_text, 'of doc ') + LENGTH('of doc '),
        STRPOS(message_text, ' discarded') - (STRPOS(message_text, 'of doc ') + LENGTH('of doc '))
    ) AS document_source,
    SUBSTRING(
        message_text,
        STRPOS(message_text, 'Page ') + LENGTH('Page '),
        STRPOS(message_text, ' of doc ') - (STRPOS(message_text, 'Page ') + LENGTH('Page '))
    )::INTEGER AS page_number
FROM public.delivery_warning
WHERE message_genre = 'DISCARD'
    AND status IS TRUE
;
