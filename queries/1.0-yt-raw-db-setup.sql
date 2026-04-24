/*
===============================================================================
Database Setup: raw_skz_pace_db
Description: DDL script to initialize the raw data lake schema for SKZ PACE.
             Stores unmodified JSON responses directly from the YouTube API.
===============================================================================
*/

-- 1. Create Database (Run this separately if using a UI, or uncomment below)
-- CREATE DATABASE raw_skz_pace_db;

-- Connect to your database before running the tables below.

/* ===============================================================================
TABLES
===============================================================================
*/

-- 1. Processed Videos Log
CREATE TABLE IF NOT EXISTS processed_vids (
    video_id VARCHAR(50) PRIMARY KEY,
    video_format VARCHAR(20),
    first_scraped_at TIMESTAMP WITH TIME ZONE NOT NULL
);
COMMENT ON TABLE processed_vids IS 'Registry of unique video IDs and their assigned formats to avoid redundant format processing.';

-- 2. Raw Snippets and Statistics
CREATE TABLE IF NOT EXISTS snippets_and_stats (
    scraped_at TIMESTAMP WITH TIME ZONE NOT NULL PRIMARY KEY,
    video_response JSONB
);
COMMENT ON TABLE snippets_and_stats IS 'Stores the raw, batched JSON payload containing snippets and statistics from the YouTube API.';

-- 3. Raw Top Comments
CREATE TABLE IF NOT EXISTS top_comments (
    video_id VARCHAR(50),
    scraped_at TIMESTAMP WITH TIME ZONE NOT NULL,
    comment_response JSONB,
    PRIMARY KEY (video_id, scraped_at),
    CONSTRAINT fk_top_comments FOREIGN KEY (video_id) 
        REFERENCES processed_vids(video_id) ON DELETE CASCADE
);
COMMENT ON TABLE top_comments IS 'Stores the raw JSON payload of top comments for a specific video at a specific extraction time.';

/* ===============================================================================
INDEXES
===============================================================================
*/

CREATE INDEX IF NOT EXISTS idx_processed_vids_format ON processed_vids(video_format);
CREATE INDEX IF NOT EXISTS idx_top_comments_video_id ON top_comments(video_id);