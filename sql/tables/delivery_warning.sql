CREATE TABLE IF NOT EXISTS wheeck.delivery_warning (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    message_genre VARCHAR(255) NOT NULL,
    message_text VARCHAR(512) NOT NULL,
    message_date TIMESTAMP NOT NULL DEFAULT NOW(),
    status BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_delivery_warning_id
        PRIMARY KEY (id)
)
;

CREATE INDEX idx_delivery_warning_message_text
    ON wheeck.delivery_warning (message_text)
;
