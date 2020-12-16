DROP TABLE IF EXISTS _dict; -- must be first, before drop extension
DROP EXTENSION IF EXISTS pgbdd CASCADE;
CREATE EXTENSION pgbdd;

-- create the main dictionary table
CREATE TABLE _dict (name VARCHAR(20), dict DICTIONARY);
-- create the 'wdc_dict' dictionary
INSERT INTO _dict(name,dict) VALUES('wdc_dict',dictionary(''));

DROP TABLE IF EXISTS dictstring;

DROP MATERIALIZED VIEW IF EXISTS wdc_eng_offer_unc;
DROP MATERIALIZED VIEW IF EXISTS denorm_p_price;
DROP VIEW IF EXISTS p_price_alts;
DROP VIEW IF EXISTS p_price_vars;
DROP VIEW IF EXISTS denorm_p_price_raw;
DROP MATERIALIZED VIEW IF EXISTS denorm_p_name;
DROP VIEW IF EXISTS p_name_alts;
DROP VIEW IF EXISTS p_name_vars;
DROP VIEW IF EXISTS denorm_p_name_raw;

CREATE TABLE dictstring (attr TEXT, vardef TEXT);

--======================--
--== Attribute p_name ==--
--======================--

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
	SELECT cluster_id, p_name_key, var||'='||alt AS alt, var||'='||alt||':'||(1.0/cnt) AS altdef
	FROM (
			SELECT p.cluster_id, p_name_key, cnt, var,
				ROW_NUMBER() OVER (PARTITION BY p.cluster_id) AS alt
			FROM denorm_p_name_raw p JOIN p_name_vars v USING ( cluster_id )
		) AS x
;

-- Add the random variable assignments to the dictionary
INSERT INTO dictstring (attr,vardef)
	SELECT 'p_name' AS attr, string_agg(altdef,';') AS vardef
	FROM p_name_alts
	-- WHERE cluster_id < 100000 -- only first 100 clusters
;

UPDATE _dict
SET dict = add(dict, vardef)
FROM dictstring
WHERE name = 'wdc_dict'
  AND attr = 'p_name'
;

-- Create final denormalised table for p_name attribute with sentence
CREATE MATERIALIZED VIEW denorm_p_name AS
	SELECT p.*, CASE WHEN a.alt IS NULL THEN bdd('1') ELSE bdd(a.alt) END AS _sentence
	FROM denorm_p_name_raw p FULL OUTER JOIN p_name_alts a USING ( p_name_key )
--	WHERE p.cluster_id<=304
;

--=======================--
--== Attribute p_price ==--
--=======================--

-- View for the denormalized table containing all values for only the attribute p_price
CREATE VIEW denorm_p_price_raw AS
	SELECT *, ROW_NUMBER() OVER () AS p_price_key
	FROM (
		SELECT DISTINCT cluster_id, p_price
		FROM public.wdc_eng_offer
		WHERE p_price IS NOT NULL
		ORDER BY cluster_id
	) dppr
;

-- View for establishing the necessary random variables for the uncertainty
-- caused by conflicting p_price values within each cluster
CREATE VIEW p_price_vars AS
	SELECT *, 'pp'||varnum AS var
	FROM (
			SELECT cluster_id, COUNT(*) AS cnt,
				ROW_NUMBER() OVER (ORDER BY cluster_id) AS varnum
			FROM denorm_p_price_raw
			GROUP BY cluster_id
			HAVING COUNT(*)>1
		) AS b
;

-- View for the construction of the associated random variable assignments
-- associated with each alternative value of p_price for each cluster
CREATE VIEW p_price_alts AS
	SELECT cluster_id, p_price_key, var||'='||alt AS alt, var||'='||alt||':'||(1.0/cnt) AS altdef
	FROM (
			SELECT p.cluster_id, p_price_key, cnt, var,
				ROW_NUMBER() OVER (PARTITION BY p.cluster_id) AS alt
			FROM denorm_p_price_raw p JOIN p_price_vars v USING ( cluster_id )
		) AS x
;

-- Add the random variable assignments to the dictionary
INSERT INTO dictstring (attr,vardef)
	SELECT 'p_price' AS attr, string_agg(altdef,';') AS vardef
	FROM p_price_alts
	-- WHERE cluster_id < 100000 -- only first 100 clusters
;

UPDATE _dict
SET dict = add(dict, vardef)
FROM dictstring
WHERE name = 'wdc_dict'
  AND attr = 'p_price'
;

-- Create final denormalised table for p_name attribute with sentence
CREATE MATERIALIZED VIEW denorm_p_price AS
	SELECT p.*, CASE WHEN a.alt IS NULL THEN bdd('1') ELSE bdd(a.alt) END AS _sentence
	FROM denorm_p_price_raw p FULL OUTER JOIN p_price_alts a USING ( p_price_key )
--	WHERE p.cluster_id<=304
;

--=================================--
--== Re-join denormalized tables ==--
--=================================--

CREATE MATERIALIZED VIEW wdc_eng_offer_unc AS
	SELECT COALESCE(n.cluster_id, p.cluster_id) AS cluster_id,
		   p_name, p_price, 
		   CASE WHEN n._sentence IS NULL THEN p._sentence
		        WHEN p._sentence IS NULL THEN n._sentence
				ELSE n._sentence & p._sentence 
			END AS _sentence
	FROM denorm_p_name n FULL OUTER JOIN denorm_p_price p USING ( cluster_id )
	WHERE COALESCE(p.cluster_id, n.cluster_id) < 1000
;


-- SELECT regexp_split_to_table( print(dict), ';\s*')
-- FROM _dict 
-- WHERE name = 'wdc_dict';

