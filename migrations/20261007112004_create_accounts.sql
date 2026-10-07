-- +goose Up
SELECT 'up SQL query';

-- Create the top-level account boundary used to scope
-- Anzu data when multiple users/accounts are introduced.
CREATE TABLE accounts (
    id UUID PRIMARY KEY,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down

-- -- Remove the accounts table when rolling this migration back.
-- DROP TABLE accounts;