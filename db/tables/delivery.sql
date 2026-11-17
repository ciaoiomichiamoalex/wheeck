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
    CONSTRAINT chk_delivery_distance
        CHECK (distance >= 0),
    CONSTRAINT chk_delivery_page_number
        CHECK (page_number > 0)
)
;

CREATE INDEX idx_delivery_company_name
    ON wheeck.delivery (company_name)
;
CREATE INDEX idx_delivery_delivery_city
    ON wheeck.delivery (delivery_city)
;
