-- 1. Snippets (Static video metadata)
CREATE TABLE skz_snippets (
    video_id VARCHAR(50) PRIMARY KEY,
    published_at TIMESTAMP WITH TIME ZONE,
    video_format VARCHAR(20),
    title TEXT,
    description TEXT,
    category_id VARCHAR(10),
    tags TEXT,
    scraped_at TIMESTAMP WITH TIME ZONE
);

-- 2. Transcripts (One-to-one with snippets)
CREATE TABLE skz_transcripts (
    video_id VARCHAR(50) PRIMARY KEY REFERENCES skz_snippets(video_id),
    transcript TEXT
);

-- 3. Stats (Time-series data, tracked over time)
CREATE TABLE skz_stats (
    video_id VARCHAR(50) REFERENCES skz_snippets(video_id),
    scraped_at TIMESTAMP WITH TIME ZONE,
    view_count BIGINT,
    like_count BIGINT,
    comment_count BIGINT,
    PRIMARY KEY (video_id, scraped_at)
);

-- 4. Comments (Snapshots of top comments, tracked over time)
CREATE TABLE skz_comments (
    comment_id VARCHAR(100),
    video_id VARCHAR(50) REFERENCES skz_snippets(video_id),
    author VARCHAR(100),
    text TEXT,
    like_count INT,
    published_at TIMESTAMP WITH TIME ZONE,
    scraped_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (comment_id, scraped_at)
);