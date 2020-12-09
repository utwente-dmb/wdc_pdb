DROP VIEW IF EXISTS denorm_p_name_raw;
CREATE VIEW denorm_p_name_raw AS
	SELECT DISTINCT cluster_id, p_name
	FROM public.wdc_eng_offer
	WHERE p_name IS NOT NULL
	ORDER BY cluster_id;

DROP VIEW IF EXISTS p_name_vars;
CREATE VIEW p_name_vars AS
	SELECT *, 'pn'||varnum AS var
	FROM (
			SELECT cluster_id, COUNT(*) AS cnt,
				ROW_NUMBER() OVER (ORDER BY cluster_id) AS varnum
			FROM denorm_p_name_raw
			GROUP BY cluster_id
			HAVING COUNT(*)>1
		) AS b;
		