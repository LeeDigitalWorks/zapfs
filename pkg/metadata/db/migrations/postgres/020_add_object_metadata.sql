-- Migration: Add user metadata column to objects table
-- Stores x-amz-meta-* headers as JSON
ALTER TABLE objects ADD COLUMN metadata JSONB DEFAULT NULL;
COMMENT ON COLUMN objects.metadata IS 'User-defined metadata (x-amz-meta-* headers)';
