SELECT * FROM youtube_schema.staging_youtubestats

CREATE TABLE youtube_schema.staging_youtubestats (
    load_date TIMESTAMP,
    sysdate JSONB
);

INSERT INTO youtube_schema.staging_youtubestats (load_date, sysdate)
SELECT 
    NOW() AS load_date, 
    jsonb_build_object(
        'rank', rank,
        'youtuber', youtuber,
        'subscribers', subscribers,
        'video_views', video_views,
        'category', category,
        'title', title,
        'uploads', uploads,
        'country', country,
        'abbreviation', abbreviation,
        'channel_type', channel_type,
        'video_views_rank', video_views_rank,
        'country_rank', country_rank,
        'channel_type_rank', channel_type_rank,
        'video_views_for_last_30_days', video_views_for_last_30_days,
        'lowest_monthly_earnings', lowest_monthly_earnings,
        'highest_monthly_earnings', highest_monthly_earnings,
        'lowest_yearly_earnings', lowest_yearly_earnings,
        'highest_yearly_earnings', highest_yearly_earnings,
        'subscribers_for_last_30_days', subscribers_for_last_30_days,
        'created_year', created_year,
        'created_month', created_month,
        'created_date', created_date,
        'gross_tertiary_education_enrollment', gross_tertiary_education_enrollment,
        'population', population,
        'unemployment_rate', unemployment_rate,
        'urban_population', urban_population,
        'latitude', latitude,
        'longitude', longitude
    ) AS sysdate
FROM youtube_schema.staging_youtube_stats;

SELECT * FROM youtube_schema.staging_youtube_stats

CREATE TABLE youtube_schema.countries (
    countryID INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    country VARCHAR(255),
    abbreviation VARCHAR(5),
    population BIGINT,
    unemployment_rate DECIMAL(5,2),
    urban_population BIGINT,
    gross_tertiary_enrollment DECIMAL(5,2),
    latitude DECIMAL(8,6),
    longitude DECIMAL(9,6)
);

CREATE TABLE youtube_schema.channel_types (
    channel_typeID INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    channel_type VARCHAR(255)
);

CREATE TABLE youtube_schema.channels (
    channelID INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rank INT,
    youtuber VARCHAR(255),
    subscribers BIGINT,
    video_views DOUBLE PRECISION,
    categoryID INT,
    title VARCHAR(255),
    uploads INT,
    created_year INT,
    created_month VARCHAR(10),
    created_date INT,
    countryID INT,
    channel_typeID INT
);


CREATE TABLE youtube_schema.ranks (
    rankID INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    channelID INT,
    video_views_rank NUMERIC(10,2),
    country_rank NUMERIC(10,2),
    channel_type_rank NUMERIC(10,2)
);

CREATE TABLE youtube_schema.earnings (
    earningsID INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    channelID INT,
    lowest_monthly_earnings DECIMAL(15,2),
    highest_monthly_earnings DECIMAL(15,2),
    lowest_yearly_earnings DECIMAL(15,2),
    highest_yearly_earnings DECIMAL(15,2)
);

CREATE TABLE youtube_schema.growth_metrics (
    growthID INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    channelID INT,
    video_views_for_last_30_days BIGINT,
    subscribers_for_last_30_days BIGINT
);

CREATE TABLE youtube_schema.categories (
	categoryID INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	category VARCHAR(255)
);

DROP TABLE youtube_schema.categories;
DROP TABLE youtube_schema.channel_types;
DROP TABLE youtube_schema.channels;
DROP TABLE youtube_schema.countries;
DROP TABLE youtube_schema.earnings;
DROP TABLE youtube_schema.growth_metrics;
DROP TABLE youtube_schema.ranks;

ALTER TABLE youtube_schema.countries DROP COLUMN countryID;
ALTER TABLE youtube_schema.countries
ADD COLUMN countryID INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY;

INSERT INTO youtube_schema.countries (country, abbreviation, population, unemployment_rate, urban_population, gross_tertiary_enrollment, latitude, longitude)
SELECT DISTINCT
    sysdate->>'country' AS country,
    sysdate->>'abbreviation' AS abbreviation,
    (sysdate->>'population')::BIGINT AS population,
    (sysdate->>'unemployment_rate')::NUMERIC(5,2) AS unemployment_rate,
    (sysdate->>'urban_population')::BIGINT AS urban_population,
    (sysdate->>'gross_tertiary_education_enrollment')::NUMERIC(5,2) AS gross_tertiary_enrollment,
    (sysdate->>'latitude')::NUMERIC(9,6) AS latitude,
    (sysdate->>'longitude')::NUMERIC(9,6) AS longitude
FROM youtube_schema.staging_youtubestats;

INSERT INTO youtube_schema.channel_types (channel_type)
SELECT DISTINCT
    sysdate->>'channel_type' AS channel_type
FROM youtube_schema.staging_youtubestats;

SELECT * FROM youtube_schema.channel_types;

INSERT INTO youtube_schema.categories (category)
SELECT DISTINCT
    sysdate->>'category' AS category
FROM youtube_schema.staging_youtubestats;
SELECT * FROM youtube_schema.categories;

INSERT INTO youtube_schema.channels (rank, youtuber, subscribers, video_views, categoryID, title, uploads, created_year, created_month, created_date, countryID, channel_typeID)
SELECT DISTINCT
    (sysdate->>'rank')::INT AS rank,
    sysdate->>'youtuber' AS youtuber,
    (sysdate->>'subscribers')::BIGINT AS subscribers,
    (sysdate->>'video_views')::DOUBLE PRECISION AS video_views,
	cat.categoryID,
    sysdate->>'title' AS title,
    (sysdate->>'uploads')::INT AS uploads,
    (sysdate->>'created_year')::INT AS created_year,
    sysdate->>'created_month' AS created_month,
    (sysdate->>'created_date')::INT AS created_date,
	c.countryID,
	ct.channel_typeID
FROM youtube_schema.staging_youtubestats
JOIN youtube_schema.countries c
	ON sysdate->>'country' = c.country
JOIN youtube_schema.categories cat
	ON sysdate->>'category' = cat.category
JOIN youtube_schema.channel_types ct
	ON sysdate->>'channel_type'=ct.channel_type;
	
SELECT * FROM youtube_schema.categories;

INSERT INTO youtube_schema.earnings ( channelID, lowest_monthly_earnings, highest_monthly_earnings, lowest_yearly_earnings, highest_yearly_earnings)
SELECT DISTINCT
	c.channelID,
    (sysdate->>'lowest_monthly_earnings')::NUMERIC(15,2) AS lowest_monthly_earnings,
    (sysdate->>'highest_monthly_earnings')::NUMERIC(15,2) AS highest_monthly_earnings,
    (sysdate->>'lowest_yearly_earnings')::NUMERIC(15,2) AS lowest_yearly_earnings,
    (sysdate->>'highest_yearly_earnings')::NUMERIC(15,2) AS highest_yearly_earnings
FROM youtube_schema.staging_youtubestats sysdate
JOIN youtube_schema.channels c
  ON sysdate->>'title' = c.title;
SELECT * FROM youtube_schema.earnings;

INSERT INTO youtube_schema.growth_metrics (channelID, video_views_for_last_30_days, subscribers_for_last_30_days)
SELECT DISTINCT
    c.channelID, 
    (sysdate->>'video_views_for_last_30_days')::BIGINT AS video_views_for_last_30_days,
    (sysdate->>'subscribers_for_last_30_days')::BIGINT AS subscribers_for_last_30_days
FROM youtube_schema.staging_youtubestats sysdate
JOIN youtube_schema.channels c
  ON sysdate->>'title' = c.title;
SELECT * FROM youtube_schema.growth_metrics;

INSERT INTO youtube_schema.ranks (channelID, video_views_rank, country_rank, channel_type_rank)
SELECT DISTINCT
    c.channelID,  
    (sysdate->>'video_views_rank')::NUMERIC(10,2) AS video_views_rank,
    (sysdate->>'country_rank')::NUMERIC(10,2) AS country_rank,
    (sysdate->>'channel_type_rank')::NUMERIC(10,2) AS channel_type_rank
FROM youtube_schema.staging_youtubestats sysdate
JOIN youtube_schema.channels c
  ON sysdate->>'youtuber' = c.youtuber;
SELECT * FROM youtube_schema.ranks;

UPDATE youtube_schema.ranks r
SET channelID = c.channelID
FROM youtube_schema.channels c
WHERE r.rankID = c.rank;

UPDATE youtube_schema.channels c
SET channel_typeID = ct.channel_typeID
FROM youtube_schema.staging_youtube_stats ys
JOIN youtube_schema.channel_types ct
  ON ys.channel_type = ct.channel_type
WHERE c.youtuber = ys.youtuber;

UPDATE youtube_schema.channels c
SET categoryID = ct.categoryID
FROM youtube_schema.staging_youtube_stats ys
JOIN youtube_schema.categories ct
  ON ys.category = ct.category
WHERE c.youtuber = ys.youtuber;

SELECT * FROM youtube_schema.countries;

TRUNCATE TABLE youtube_schema.countries;