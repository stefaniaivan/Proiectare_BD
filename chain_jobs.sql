SELECT proname FROM pg_proc WHERE proname = 'starts_with';

SELECT * FROM timetable.task;

CREATE OR REPLACE FUNCTION starts_with(text, text) RETURNS boolean AS $$
SELECT position($2 IN $1) = 1;
$$ LANGUAGE sql IMMUTABLE;

INSERT INTO timetable.chain (chain_name, run_at, live)
VALUES ('Test Job Every Minute', '@every 1 minute', TRUE);

INSERT INTO timetable.task (chain_id, task_order, task_name, kind, command, autonomous)
VALUES (
    (SELECT chain_id FROM timetable.chain WHERE chain_name = 'Test Job Every Minute'),  
    1,                                            --ordinea task-ului in chain
    'Insert log entry',                           --numele sarcinii
    'SQL',                                        --tipul sarcinii
    'INSERT INTO youtube_schema.task_logs (log_time, message) VALUES (NOW(), ''Task-ul a fost executat cu succes'')',  -- comanda de executat
    TRUE                                          --indica daca task-ul este autonom
);

CREATE TABLE youtube_schema.task_logs(
    log_id SERIAL PRIMARY KEY,
    log_time TIMESTAMP,
    message TEXT
);

SELECT * FROM youtube_schema.staging_youtubestats;

--de inserat in fiecare tabel prin joburi la 3 minute din tabelul de staging, se insereaza datele pe parti
--la intervalul de timp dat 

ALTER TABLE youtube_schema.staging_youtubestats
ADD COLUMN processed BOOLEAN DEFAULT FALSE;

INSERT INTO timetable.chain (chain_name, run_at, live)
VALUES ('Insert Data Every 3 minutes', '@every 3 minutes', TRUE);

INSERT INTO timetable.task (chain_id, task_order, task_name, kind, command, autonomous)
VALUES (
    (SELECT chain_id FROM timetable.chain WHERE chain_name = 'Insert Data Every 3 minutes'),
    1,
    'Insert into countries',
    'SQL',
    $$
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
	FROM (
    	SELECT * 
    	FROM youtube_schema.staging_youtubestats
    	WHERE processed = FALSE
    	LIMIT 200
	) AS limited_data
	WHERE NOT EXISTS (
    	SELECT 1
    	FROM youtube_schema.countries
    	WHERE countries.abbreviation = limited_data.sysdate->>'abbreviation'
		)
    $$,
    TRUE
);

INSERT INTO timetable.task (chain_id, task_order, task_name, kind, command, autonomous)
VALUES (
    (SELECT chain_id FROM timetable.chain WHERE chain_name = 'Insert Data Every 3 minutes'),
    2,
    'Insert into channel_types',
    'SQL',
    $$
    INSERT INTO youtube_schema.channel_types (channel_type)
	SELECT DISTINCT
    	sysdate->>'channel_type' AS channel_type
	FROM (
    	SELECT * 
    	FROM youtube_schema.staging_youtubestats
    	WHERE processed = FALSE
    	LIMIT 200
	) AS limited_data
	WHERE NOT EXISTS (
    	SELECT 1
    	FROM youtube_schema.channel_types
    	WHERE channel_types.channel_type = limited_data.sysdate->>'channel_type'
		)
    $$,
    TRUE
);

INSERT INTO timetable.task (chain_id, task_order, task_name, kind, command, autonomous)
VALUES (
    (SELECT chain_id FROM timetable.chain WHERE chain_name = 'Insert Data Every 3 minutes'),
    3,
    'Insert into categories',
    'SQL',
    $$
    INSERT INTO youtube_schema.categories (category)
	SELECT DISTINCT
    	sysdate->>'category' AS category
	FROM (
    	SELECT * 
    	FROM youtube_schema.staging_youtubestats
    	WHERE processed = FALSE
    	LIMIT 200
	) AS limited_data
	WHERE NOT EXISTS (
    	SELECT 1
    	FROM youtube_schema.categories
    	WHERE categories.category = limited_data.sysdate->>'category'
		)
    $$,
    TRUE
);

INSERT INTO timetable.task (chain_id, task_order, task_name, kind, command, autonomous)
VALUES (
    (SELECT chain_id FROM timetable.chain WHERE chain_name = 'Insert Data Every 3 minutes'),
    4,
    'Insert into channels',
    'SQL',
    $$
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
	FROM (
    	SELECT * 
    	FROM youtube_schema.staging_youtubestats
    	WHERE processed = FALSE
    	LIMIT 200
	) AS limited_data
	JOIN youtube_schema.countries c
	ON sysdate->>'country' = c.country
	JOIN youtube_schema.categories cat
	ON sysdate->>'category' = cat.category
	JOIN youtube_schema.channel_types ct
	ON sysdate->>'channel_type'=ct.channel_type
	
	WHERE NOT EXISTS (
    	SELECT 1
    	FROM youtube_schema.channels
    	WHERE channels.title = limited_data.sysdate->>'title'
		)
    $$,
    TRUE
);

INSERT INTO timetable.task (chain_id, task_order, task_name, kind, command, autonomous)
VALUES (
    (SELECT chain_id FROM timetable.chain WHERE chain_name = 'Insert Data Every 3 minutes'),
    5,
    'Insert into earnings',
    'SQL',
    $$
    INSERT INTO youtube_schema.earnings ( channelID, lowest_monthly_earnings, highest_monthly_earnings, lowest_yearly_earnings, highest_yearly_earnings)
	SELECT DISTINCT
		c.channelID,
    	(sysdate->>'lowest_monthly_earnings')::NUMERIC(15,2) AS lowest_monthly_earnings,
    	(sysdate->>'highest_monthly_earnings')::NUMERIC(15,2) AS highest_monthly_earnings,
    	(sysdate->>'lowest_yearly_earnings')::NUMERIC(15,2) AS lowest_yearly_earnings,
    	(sysdate->>'highest_yearly_earnings')::NUMERIC(15,2) AS highest_yearly_earnings
	FROM (
    	SELECT * 
    	FROM youtube_schema.staging_youtubestats
    	WHERE processed = FALSE
    	LIMIT 200
	) AS limited_data
	JOIN youtube_schema.channels c
 	ON sysdate->>'title' = c.title
	 
	WHERE NOT EXISTS (
    	SELECT 1
    	FROM youtube_schema.earnings
    	WHERE earnings.channelID = c.channelID
	)
    $$,
    TRUE
);

INSERT INTO timetable.task (chain_id, task_order, task_name, kind, command, autonomous)
VALUES (
    (SELECT chain_id FROM timetable.chain WHERE chain_name = 'Insert Data Every 3 minutes'),
    6,
    'Insert into growth_metrics',
    'SQL',
    $$
    INSERT INTO youtube_schema.growth_metrics (channelID, video_views_for_last_30_days, subscribers_for_last_30_days)
	SELECT DISTINCT
    	c.channelID, 
    	(sysdate->>'video_views_for_last_30_days')::BIGINT AS video_views_for_last_30_days,
    	(sysdate->>'subscribers_for_last_30_days')::BIGINT AS subscribers_for_last_30_days
	FROM (
    	SELECT * 
    	FROM youtube_schema.staging_youtubestats
    	WHERE processed = FALSE
    	LIMIT 200
	) AS limited_data
	JOIN youtube_schema.channels c
  	ON sysdate->>'title' = c.title
	  
	WHERE NOT EXISTS (
    	SELECT 1
    	FROM youtube_schema.growth_metrics
    	WHERE growth_metrics.channelID = c.channelID
		)
    $$,
    TRUE
);

INSERT INTO timetable.task (chain_id, task_order, task_name, kind, command, autonomous)
VALUES (
    (SELECT chain_id FROM timetable.chain WHERE chain_name = 'Insert Data Every 3 minutes'),
    7,
    'Insert into ranks',
    'SQL',
    $$
    INSERT INTO youtube_schema.ranks (channelID, video_views_rank, country_rank, channel_type_rank)
	SELECT DISTINCT
    	c.channelID,  
    	(sysdate->>'video_views_rank')::NUMERIC(10,2) AS video_views_rank,
    	(sysdate->>'country_rank')::NUMERIC(10,2) AS country_rank,
    	(sysdate->>'channel_type_rank')::NUMERIC(10,2) AS channel_type_rank
	FROM (
    	SELECT * 
    	FROM youtube_schema.staging_youtubestats
    	WHERE processed = FALSE
    	LIMIT 200
	) AS limited_data
	JOIN youtube_schema.channels c
  	ON sysdate->>'title' = c.title
	  
	WHERE NOT EXISTS (
    	SELECT 1
    	FROM youtube_schema.ranks
    	WHERE ranks.channelID = c.channelID
		);

	UPDATE youtube_schema.staging_youtubestats
	SET processed = TRUE
	WHERE ctid IN (
    	SELECT ctid
    	FROM youtube_schema.staging_youtubestats
    	WHERE processed = FALSE
    LIMIT 200
	)
    $$,
    TRUE
);

INSERT INTO timetable.task (chain_id, task_order, task_name, kind, command, autonomous)
VALUES (
    (SELECT chain_id FROM timetable.chain WHERE chain_name = 'Insert Data Every 30 seconds'),
    100,
    'Log successful insertion',
    'SQL',
    'INSERT INTO youtube_schema.task_logs (log_time, message) 
     VALUES (NOW(), ''200 records processed for Insert Data Every 30 seconds job chain'');',
    TRUE
);

UPDATE youtube_schema.staging_youtubestats
SET processed = FALSE;