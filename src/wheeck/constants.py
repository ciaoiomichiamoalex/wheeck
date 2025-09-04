from datetime import date
from pathlib import Path

PATH_PRJ = Path(__file__).resolve().parents[2]
PATH_CFG = PATH_PRJ / 'config'
PATH_LOG = PATH_PRJ / 'log'
PATH_RES = PATH_PRJ / 'res'
PATH_SCHEME = PATH_PRJ / 'scheme'

PATH_CFG_PRJ = PATH_CFG / 'wheeck.json'

PATH_WORKING_DIR = PATH_PRJ / 'DDTs'
PATH_DISCARDED_DIR = PATH_WORKING_DIR / 'discarded'
PATH_RECORDED_DIR = PATH_WORKING_DIR / 'recorded'

PATTERN_WORKING_DOC = r'^\d{4}_\d{2}_DDT_\d{4}_\d{4}(?:_P\d{3})*\.pdf$'
PATTERN_DISCARD_DOC = r'^(\d{4}_\d{2}_DDT_\d{4}_\d{4})_P(\d{3})(?:_P\d{3})*\.pdf$'

PATTERN_DOC_NUMBER = r'Num\. D\.D\.T\. ([\d\.]+)\/(\w{2}) Data D\.D\.T\. (\d{2}\/\d{2}\/\d{4}) Pag'
PATTERN_CITY_DX = r"Luogo di consegna\r\n([\w\t \.\&\-'\/\(\)]+)\r\n.+\r\n(?:\d{0,5}) ?([\w\t '\-\.]+) \(?(?:\w{2})\)?\r\nTelefono"
PATTERN_CITY_SX = r"Luogo di partenza: .+\r\n([\w\t \.\&\-'\/]+)\r\n(?:\d{5}) ([\w\t '\-\.]+) \(?(?:\w{2})\)?\r\n"
PATTERN_QUANTITY = r'(?:Quantità Prezzo\r\n.+)? (?:L|KG) ([\d\.]+),000\s'
PATTERN_VEHICLE = r'Peso soggetto accisa\r\n([\w\d]{7})\r\n([\w ]+)?\r?\n?Targa automezzo'

CMD_BACKUP_DB = f'pg_dump -U postgres -h 127.0.0.1 -p 5432 -d postgres -n wheeck -w -c -F c -f \"{PATH_SCHEME}/wheeck.bak.dump\"'

OVERVIEW_DEFAULT_FONT = 'Arial'
OVERVIEW_FORMATS = {
    type(None): 'General',
    str: '@',
    int: '#,##0',
    date: 'dd/mm'
}

QUERY_INSERT_DELIVERY = """\
    INSERT INTO wheeck.delivery (
        document_number,
        document_genre,
        document_date,
        company_name,
        delivery_city,
        quantity,
        delivery_date,
        vehicle,
        vehicle_driver,
        distance,
        document_source,
        page_number,
        recording_date
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ;
"""

QUERY_CHECK_DUPLICATE = """\
    SELECT COUNT(*) AS nr_record
    FROM wheeck.delivery
    WHERE (
        document_source = ?
        AND page_number = ?
    ) OR (
        document_number = ?
        AND document_genre = ?
        AND EXTRACT(YEAR FROM document_date) = ?
    )
    ;
"""

QUERY_GET_DISTANCE = """\
    SELECT DISTINCT distance
    FROM wheeck.delivery
    WHERE delivery_city = ?
    ;
"""

QUERY_GET_DISCARD = """\
    SELECT document_number,
        document_genre,
        document_date,
        company_name,
        delivery_city,
        quantity,
        delivery_date,
        vehicle,
        vehicle_driver,
        distance,
        id_warning_message
    FROM wheeck.delivery_discard
    WHERE status IS TRUE
        AND document_source = ?
        AND page_number = ?
    ;
"""
QUERY_INSERT_DISCARD = """\
    INSERT INTO wheeck.delivery_discard (
        document_number,
        document_genre,
        document_date,
        company_name,
        delivery_city,
        quantity,
        delivery_date,
        vehicle,
        vehicle_driver,
        distance,
        document_source,
        page_number,
        recording_date,
        id_warning_message
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ;
"""

QUERY_INSERT_WARNING = """\
    INSERT INTO wheeck.delivery_warning (
        message_genre,
        message_text
    ) VALUES (?, ?)
    RETURNING id
    ;
"""
QUERY_UPDATE_WARNING = """\
    UPDATE wheeck.delivery_warning
    SET status = FALSE
    WHERE id = ?
    ;
"""

QUERY_CHECK_GAPS = """\
    SELECT dg.document_number,
        dg.document_year
    FROM wheeck.vw_delivery_gap dg
        LEFT JOIN wheeck.vw_gap_message gm
            ON dg.document_number = gm.document_number
            AND dg.document_year = gm.document_year
    WHERE dg.is_discard IS FALSE
        AND gm.document_number IS NULL
    ORDER BY dg.document_year, dg.document_number
    ;
"""
QUERY_GET_GAP = """\
    SELECT id
    FROM wheeck.vw_gap_message
    WHERE document_number = ?
        AND document_year = ?
    ;
"""

QUERY_GET_OVERVIEW = """\
    SELECT document_number,
        document_date,
        company_name,
        delivery_city,
        quantity,
        delivery_date,
        vehicle
    FROM wheeck.delivery
    WHERE EXTRACT(YEAR FROM delivery_date) = ?
        AND EXTRACT(MONTH FROM delivery_date) = ?
    ORDER BY document_number
    ;
"""
QUERY_GET_SUMMARY = """\
    WITH year_summary AS (
        SELECT document_number,
            document_date,
            delivery_city,
            delivery_date,
            vehicle,
            ROW_NUMBER() OVER (
                PARTITION BY delivery_date, vehicle 
                ORDER BY distance DESC
            ) AS rnk
        FROM wheeck.delivery
        WHERE EXTRACT(YEAR FROM delivery_date) = ?
            AND vehicle_driver NOT IN (%s)
    ), summary0 AS (
        SELECT document_number,
            document_date,
            delivery_city,
            delivery_date,
            ROW_NUMBER() OVER (
                PARTITION BY vehicle
                ORDER BY delivery_date
            ) AS row_num
        FROM year_summary 
        WHERE rnk = 1
            AND vehicle = ?
    ), summary1 AS (
        SELECT document_number,
            document_date,
            delivery_city,
            delivery_date,
            ROW_NUMBER() OVER (
                PARTITION BY vehicle
                ORDER BY delivery_date
            ) AS row_num
        FROM year_summary 
        WHERE rnk = 1
            AND vehicle = ?
    )
    SELECT s0.document_number AS document_number_0,
        s0.document_date AS document_date_0,
        s0.delivery_city AS delivery_city_0,
        s0.delivery_date AS delivery_date_0,
        NULL AS gap,
        s1.delivery_date AS delivery_date_1,
        s1.delivery_city AS delivery_city_1,
        s1.document_date AS document_date_1,
        s1.document_number AS document_number_1
    FROM summary0 s0
        FULL JOIN summary1 s1 ON s0.row_num = s1.row_num
    ORDER BY COALESCE(s0.row_num, s1.row_num)
    ;
"""
QUERY_GET_LAST_OVERVIEWS = """\
    SELECT DISTINCT EXTRACT(YEAR FROM delivery_date)::INT AS year,
        EXTRACT(MONTH FROM delivery_date)::INT AS month
    FROM wheeck.delivery 
    WHERE recording_date >= NOW() - INTERVAL '1' DAY
        AND recording_date < NOW()
    ORDER BY 1, 2
    ;
"""
