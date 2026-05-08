-- ===========================================================================
-- FEATURE ENGINEERING
-- ===========================================================================
-- This retrieves Columns: video_id, title, video_format, video_age_days,
--                         like_to_view_ratio, marginal_ratio, daily_view_velocity
WITH ranked_stats AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY scraped_at ASC) as entry_rank,
        ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY scraped_at DESC) as exit_rank
    FROM skz_stats
)

SELECT
    ranked_stats.video_id,
    skz_snippets.title,
    skz_snippets.video_format,
    CASE
        WHEN
            EXTRACT(DAY FROM
                            skz_snippets.scraped_at -
                            skz_snippets.published_at
            ) < 30 THEN 'New Release (<30 Days)'
        WHEN
            EXTRACT(DAY FROM
                            skz_snippets.scraped_at -
                            skz_snippets.published_at
            ) < 180 THEN 'Recent (1-6 Months)'
        WHEN
            EXTRACT(DAY FROM
                            skz_snippets.scraped_at -
                            skz_snippets.published_at
            ) < 720 THEN 'Catalog (6-24 Months)'
        ELSE 'Legacy (2+ Years)'
    END AS video_age_days,
    MAX(like_count::REAL) FILTER (WHERE exit_rank = 1) / 
    MAX(view_count::REAL) FILTER (WHERE exit_rank = 1) AS like_to_view_ratio,
    (
        MAX(like_count) FILTER (WHERE exit_rank = 1) -
        MAX(like_count) FILTER (WHERE entry_rank = 1)
    )::REAL / (
        MAX(view_count) FILTER (WHERE exit_rank = 1) -
        MAX(view_count) FILTER (WHERE entry_rank = 1)
    )::REAL AS margin_like_to_view_ratio,
    (
        MAX(view_count) FILTER (WHERE exit_rank = 1) -
        MAX(view_count) FILTER (WHERE entry_rank = 1)
    )::REAL / (
        EXTRACT(
            EPOCH FROM (MAX(ranked_stats.scraped_at) -
            MIN(ranked_stats.scraped_at))
        ) / 86400
    ) AS daily_view_velocity
FROM ranked_stats
LEFT JOIN skz_snippets
    ON skz_snippets.video_id = ranked_stats.video_id
GROUP BY
    ranked_stats.video_id,
    skz_snippets.title,
    skz_snippets.video_format,
    video_age_days,
    skz_snippets.published_at
ORDER BY skz_snippets.published_at
LIMIT 50;
