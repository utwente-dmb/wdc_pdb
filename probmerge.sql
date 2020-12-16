DROP TABLE IF EXISTS _dict; -- must be first, before drop extension
DROP EXTENSION IF EXISTS pgbdd CASCADE;
CREATE EXTENSION pgbdd;

-- create the main dictionary table
CREATE TABLE _dict (name VARCHAR(20), dict DICTIONARY);
-- create the 'wdc_dict' dictionary
INSERT INTO _dict(name,dict) VALUES('wdc_dict',dictionary(''));

DROP MATERIALIZED VIEW IF EXISTS denorm_p_name;
DROP VIEW IF EXISTS p_name_alts;
DROP VIEW IF EXISTS p_name_vars;
DROP VIEW IF EXISTS denorm_p_name_raw;

-- View for the denormalized table containing all values for only the attribute p_name
CREATE VIEW denorm_p_name_raw AS
	SELECT *, ROW_NUMBER() OVER () AS p_name_key
	FROM (
		SELECT DISTINCT cluster_id, p_name
		FROM public.wdc_eng_offer
		WHERE p_name IS NOT NULL
		ORDER BY cluster_id
	) dpnr
;

-- View for establishing the necessary random variables for the uncertainty
-- caused by conflicting p_name values within each cluster
CREATE VIEW p_name_vars AS
	SELECT *, 'pn'||varnum AS var
	FROM (
			SELECT cluster_id, COUNT(*) AS cnt,
				ROW_NUMBER() OVER (ORDER BY cluster_id) AS varnum
			FROM denorm_p_name_raw
			GROUP BY cluster_id
			HAVING COUNT(*)>1
		) AS b
;

-- View for the construction of the associated random variable assignments
-- associated with each alternative value of p_name for each cluster
CREATE VIEW p_name_alts AS
	SELECT cluster_id, p_name_key, var||'='||alt||':'||(1.0/cnt) AS altdef
	FROM (
			SELECT p.cluster_id, p_name_key, cnt, var,
				ROW_NUMBER() OVER (PARTITION BY p.cluster_id) AS alt
			FROM denorm_p_name_raw p JOIN p_name_vars v ON p.cluster_id=v.cluster_id
		) AS x
;

-- Add the random variable assignments to the dictionary

-- Crashes the server
-- UPDATE _dict
-- SET dict = add(dict, (SELECT string_agg(altdef,';') FROM p_name_alts) )
-- WHERE name = 'wdc_dict'
-- ;

-- Also crashes the server
-- UPDATE _dict
-- SET dict = add(dict, vardef)
-- FROM (SELECT cluster_id, string_agg(altdef,';') AS vardef
-- 	  FROM p_name_alts
-- 	  GROUP BY cluster_id
-- 	) x
-- WHERE name = 'wdc_dict'
-- ;

DROP TABLE IF EXISTS dictstring;
SELECT string_agg(altdef,';') AS vardef
INTO dictstring
FROM p_name_alts
-- WHERE cluster_id < 100000 -- only first 100 clusters
;

UPDATE _dict
SET dict = add(dict, vardef)
FROM dictstring
WHERE name = 'wdc_dict'
;

-- SELECT regexp_split_to_table( print(dict), ';\s*')
-- FROM _dict 
-- WHERE name = 'wdc_dict';

-- ERROR:  bdd_in: varnames cannot start with a digit: "50000000000000000000"
CREATE MATERIALIZED VIEW denorm_p_name AS
	SELECT p.*, bdd(a.altdef)
	FROM denorm_p_name_raw p FULL OUTER JOIN p_name_alts a
		 ON p.p_name_key = a.p_name_key
	WHERE p.cluster_id=304
;

