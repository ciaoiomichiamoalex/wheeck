DROP SCHEMA IF EXISTS wheeck CASCADE;
CREATE SCHEMA IF NOT EXISTS wheeck;

CREATE TABLE IF NOT EXISTS wheeck.delivery (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    document_number INTEGER NOT NULL,
    document_genre CHAR(2) NOT NULL,
    document_date DATE NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    delivery_city VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    delivery_date DATE NOT NULL,
    vehicle CHAR(7) NOT NULL,
    vehicle_driver VARCHAR(255),
    distance NUMERIC(7, 2),
    document_source VARCHAR(255) NOT NULL,
    page_number INTEGER NOT NULL,
    recording_date TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_delivery_id
        PRIMARY KEY (id),
    CONSTRAINT uq_delivery_document_number_genre_year
        UNIQUE (document_number, document_genre, document_date),
    CONSTRAINT uq_delivery_document_source_page
        UNIQUE (document_source, page_number),
    CONSTRAINT chk_delivery_document_number
        CHECK (document_number > 0),
    CONSTRAINT chk_delivery_quantity
        CHECK (quantity > 0),
    CONSTRAINT chk_delivery_page_number
        CHECK (page_number > 0)
);

CREATE UNIQUE INDEX idx_delivery_document_number_genre_year
    ON wheeck.delivery (document_number, document_genre, document_date);
CREATE INDEX idx_delivery_company_name
    ON wheeck.delivery (company_name);
CREATE INDEX idx_delivery_delivery_city
    ON wheeck.delivery (delivery_city);

CREATE TABLE IF NOT EXISTS wheeck.delivery_warning (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    message_genre VARCHAR(255) NOT NULL,
    message_text VARCHAR(512) NOT NULL,
    message_date TIMESTAMP NOT NULL DEFAULT NOW(),
    status BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_delivery_warning_id
        PRIMARY KEY (id)
);

CREATE INDEX idx_delivery_warning_message_text
    ON wheeck.delivery_warning (message_text);

CREATE TABLE IF NOT EXISTS wheeck.delivery_discard (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    id_warning_message INTEGER NOT NULL,
    document_number INTEGER,
    document_genre CHAR(2),
    document_date DATE,
    company_name VARCHAR(255),
    delivery_city VARCHAR(255),
    quantity INTEGER,
    delivery_date DATE,
    vehicle CHAR(7),
    vehicle_driver VARCHAR(255),
    distance NUMERIC(7, 2),
    document_source VARCHAR(255) NOT NULL,
    page_number INTEGER NOT NULL,
    recording_date TIMESTAMP NOT NULL DEFAULT NOW(),
    status BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_delivery_discard_id
        PRIMARY KEY (id),
    CONSTRAINT fk_delivery_warning_id
        FOREIGN KEY (id_warning_message)
        REFERENCES wheeck.delivery_warning (id)
        ON DELETE RESTRICT,
    CONSTRAINT uq_delivery_discard_document_source_page
        UNIQUE (document_source, page_number),
    CONSTRAINT chk_delivery_discard_page_number
        CHECK (page_number > 0)
);

CREATE INDEX idx_delivery_discard_document_number_genre_year
    ON wheeck.delivery_discard (document_number, document_genre, document_date);
CREATE INDEX idx_delivery_discard_company_name
    ON wheeck.delivery_discard (company_name);
CREATE INDEX idx_delivery_discard_delivery_city
    ON wheeck.delivery_discard (delivery_city);

CREATE OR REPLACE FUNCTION wheeck.sync_discard_status()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE wheeck.delivery_discard
    SET status = NEW.status
    WHERE id_warning_message = NEW.id;

    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

DROP TRIGGER IF EXISTS trg_sync_discard_status ON wheeck.delivery_warning;
CREATE TRIGGER trg_sync_discard_status
AFTER UPDATE OF status ON wheeck.delivery_warning
FOR EACH ROW
WHEN (NEW.message_genre = 'DISCARD')
EXECUTE FUNCTION wheeck.sync_discard_status();

REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA wheeck FROM wheeck;
ALTER DEFAULT PRIVILEGES IN SCHEMA wheeck REVOKE SELECT, INSERT, UPDATE ON TABLES FROM wheeck;
REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA wheeck FROM wheeck;
ALTER DEFAULT PRIVILEGES IN SCHEMA wheeck REVOKE EXECUTE ON FUNCTIONS FROM wheeck;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA wheeck FROM wheeck;
ALTER DEFAULT PRIVILEGES IN SCHEMA wheeck REVOKE USAGE, SELECT ON SEQUENCES FROM wheeck;
DROP USER IF EXISTS wheeck;

CREATE USER wheeck WITH PASSWORD 'Wheeck!2024';
GRANT USAGE ON SCHEMA wheeck TO wheeck;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA wheeck TO wheeck;
ALTER DEFAULT PRIVILEGES IN SCHEMA wheeck GRANT SELECT, INSERT, UPDATE ON TABLES TO wheeck;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA wheeck TO wheeck;
ALTER DEFAULT PRIVILEGES IN SCHEMA wheeck GRANT EXECUTE ON FUNCTIONS TO wheeck;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA wheeck TO wheeck;
ALTER DEFAULT PRIVILEGES IN SCHEMA wheeck GRANT USAGE, SELECT ON SEQUENCES TO wheeck;

CREATE OR REPLACE VIEW wheeck.vw_discard_message AS
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
FROM wheeck.delivery_warning
WHERE message_genre = 'DISCARD'
    AND status IS TRUE;

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
    AND status IS TRUE;

CREATE OR REPLACE VIEW wheeck.vw_delivery_gap AS
WITH doc_nums AS (
    SELECT dn.document_year,
        s.document_number
    FROM (
        SELECT EXTRACT(YEAR FROM document_date) AS document_year,
            MIN(document_number) AS min_num,
            MAX(document_number) AS max_num
        FROM wheeck.delivery
        GROUP BY EXTRACT(YEAR FROM document_date)
    ) dn,
        GENERATE_SERIES(dn.min_num, dn.max_num, 1) s(document_number)
)
SELECT dn.document_number,
    dn.document_year,
    (
        SELECT DISTINCT EXTRACT(MONTH FROM document_date) AS document_month
        FROM wheeck.delivery
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
    LEFT JOIN wheeck.delivery d
        ON dn.document_number = d.document_number
        AND dn.document_year = EXTRACT(YEAR FROM d.document_date)
    LEFT JOIN wheeck.vw_discard_message dm
        ON dn.document_number = dm.document_number
        AND dn.document_year = EXTRACT(YEAR FROM dm.document_date)
WHERE d.document_number IS NULL
ORDER BY dn.document_year, dn.document_number;
