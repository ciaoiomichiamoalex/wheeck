DROP VIEW IF EXISTS public.vw_delivery_gap;
CREATE OR REPLACE VIEW public.vw_delivery_gap AS
WITH all_delivery AS (
    SELECT * FROM public.delivery
    UNION ALL
    SELECT * FROM public.delivery_history
), doc_nums AS (
    SELECT dn.document_year,
        s.document_number
    FROM (
        SELECT EXTRACT(YEAR FROM document_date) AS document_year,
            MIN(document_number) AS min_num,
            MAX(document_number) AS max_num
        FROM all_delivery
        GROUP BY EXTRACT(YEAR FROM document_date)
    ) dn,
        GENERATE_SERIES(dn.min_num, dn.max_num, 1) s(document_number)
)
SELECT dn.document_number,
    dn.document_year,
    (
        SELECT DISTINCT EXTRACT(MONTH FROM document_date) AS document_month
        FROM all_delivery
        WHERE document_number < dn.document_number
            AND EXTRACT(YEAR FROM document_date) = dn.document_year
        ORDER BY 1 DESC
        LIMIT 1
    ) AS document_month,
    CASE
        WHEN dm.document_number IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_discard
FROM doc_nums dn
    LEFT JOIN all_delivery d
        ON dn.document_number = d.document_number
        AND dn.document_year = EXTRACT(YEAR FROM d.document_date)
    LEFT JOIN public.vw_discard_message dm
        ON dn.document_number = dm.document_number
        AND dn.document_year = EXTRACT(YEAR FROM dm.document_date)
WHERE d.document_number IS NULL
ORDER BY dn.document_year DESC, dn.document_number
;
