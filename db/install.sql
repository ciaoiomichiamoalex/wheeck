-- Run with psql from any directory: psql -f db/install.sql (\ir is a psql meta-command; order matters)
\ir schema/wheeck.sql
\ir tables/delivery.sql
\ir tables/delivery_history.sql
\ir tables/delivery_warning.sql
\ir tables/delivery_discard.sql
\ir functions/sync_discard_status.sql
\ir triggers/trg_sync_discard_status.sql
\ir roles/wheeck.sql
\ir views/vw_discard_message.sql
\ir views/vw_gap_message.sql
\ir views/vw_delivery_gap.sql
