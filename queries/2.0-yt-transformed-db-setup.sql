/*
===============================================================================
Database Setup: transformed_skz_pace_db
Description: DDL script to initialize the YouTube analytics schema for SKZ PACE.
===============================================================================
*/

-- 1. Create Database (Run this separately if using a UI, or uncomment below)
-- CREATE DATABASE transformed_skz_pace_db;

-- Connect to your database before running the tables below.

/* ===============================================================================
TABLES
===============================================================================
*/

-- 1. Snippets (Static video metadata)
CREATE TABLE IF NOT EXISTS skz_snippets (
    video_id VARCHAR(50) PRIMARY KEY,
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
    video_format VARCHAR(20),
    title TEXT NOT NULL,
    description TEXT,
    category_id VARCHAR(10),
    tags TEXT,
    video_link VARCHAR(100),
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE skz_snippets IS 'Stores static metadata for YouTube videos.';

-- 2. Transcripts (One-to-one with snippets)
CREATE TABLE IF NOT EXISTS skz_transcripts (
    video_id VARCHAR(50) PRIMARY KEY,
    transcript TEXT,
    CONSTRAINT fk_transcript_video FOREIGN KEY (video_id) 
        REFERENCES skz_snippets(video_id) ON DELETE CASCADE
);
COMMENT ON TABLE skz_transcripts IS 'Stores English or Korean transcripts for NLP processing.';

-- 3. Stats (Time-series data, tracked over time)
CREATE TABLE IF NOT EXISTS skz_stats (
    video_id VARCHAR(50),
    scraped_at TIMESTAMP WITH TIME ZONE NOT NULL,
    view_count BIGINT DEFAULT 0,
    like_count BIGINT DEFAULT 0,
    comment_count BIGINT DEFAULT 0,
    PRIMARY KEY (video_id, scraped_at),
    CONSTRAINT fk_stats_video FOREIGN KEY (video_id) 
        REFERENCES skz_snippets(video_id) ON DELETE CASCADE
);
COMMENT ON TABLE skz_stats IS 'Stores daily time-series performance metrics per video.';

-- 4. Comments (Snapshots of top comments, tracked over time)
CREATE TABLE IF NOT EXISTS skz_top_comments (
    comment_id VARCHAR(100),
    video_id VARCHAR(50),
    author VARCHAR(100),
    text TEXT,
    like_count INT DEFAULT 0,
    published_at TIMESTAMP WITH TIME ZONE,
    scraped_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (comment_id, scraped_at),
    CONSTRAINT fk_comments_video FOREIGN KEY (video_id) 
        REFERENCES skz_snippets(video_id) ON DELETE CASCADE
);
COMMENT ON TABLE skz_top_comments IS 'Stores top-level comments for sentiment analysis.';

/* ===============================================================================
INDEXES
===============================================================================
Creating indexes on frequently queried columns (like timestamps and foreign keys)
dramatically speeds up EDA and BI dashboard queries.
*/

CREATE INDEX IF NOT EXISTS idx_snippets_published_at ON skz_snippets(published_at);
CREATE INDEX IF NOT EXISTS idx_snippets_format ON skz_snippets(video_format);
CREATE INDEX IF NOT EXISTS idx_stats_scraped_at ON skz_stats(scraped_at);
CREATE INDEX IF NOT EXISTS idx_comments_video_id ON skz_top_comments(video_id);



/* ===============================================================================
DROP TABLES
===============================================================================
Just in case.
*/
-- DROP TABLE skz_transcripts;
-- DROP TABLE skz_top_comments;
-- DROP TABLE skz_stats;
-- DROP TABLE skz_snippets;