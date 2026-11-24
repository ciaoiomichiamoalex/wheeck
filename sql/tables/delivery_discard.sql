CREATE TABLE IF NOT EXISTS public.delivery_discard (
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
        REFERENCES public.delivery_warning (id)
        ON DELETE RESTRICT,
    CONSTRAINT uq_delivery_discard_document_source_page
        UNIQUE (document_source, page_number),
    CONSTRAINT chk_delivery_discard_quantity
        CHECK (quantity > 0),
    CONSTRAINT chk_delivery_discard_distance
        CHECK (distance >= 0),
    CONSTRAINT chk_delivery_discard_page_number
        CHECK (page_number > 0)
)
;

CREATE INDEX idx_delivery_discard_document_number_genre_year
    ON public.delivery_discard (document_number, document_genre, document_date)
;
CREATE INDEX idx_delivery_discard_company_name
    ON public.delivery_discard (company_name)
;
CREATE INDEX idx_delivery_discard_delivery_city
    ON public.delivery_discard (delivery_city)
;
