-- Migration: Add user metadata column to objects table
-- Stores x-amz-meta-* headers as JSON
ALTER TABLE objects ADD COLUMN metadata JSON DEFAULT NULL COMMENT 'User-defined metadata (x-amz-meta-* headers)';
