CREATE   PROCEDURE etl.charger_dim_date AS
BEGIN
    DELETE FROM dim.date;

    WITH n AS (SELECT v FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) t(v)),
        nums AS (
            SELECT a.v + 10*b.v + 100*c.v + 1000*d.v AS i
            FROM n a 
                CROSS JOIN n b 
                CROSS JOIN n c 
                CROSS JOIN n d
        ),
        jours AS (
            SELECT DATEADD(day, i, CAST('2021-01-01' AS date)) AS d,
                    DATEDIFF(day, '1900-01-01', DATEADD(day, i, CAST('2021-01-01' AS date))) % 7 + 1 AS js
            FROM nums
            WHERE i <= DATEDIFF(day, '2021-01-01', '2027-12-31'))
    INSERT INTO dim.date (date_key, [date], annee, mois, mois_libelle, jour, jour_semaine, jour_libelle, est_weekend)
    SELECT 
        YEAR(d)*10000 + MONTH(d)*100 + DAY(d),
        d, 
        YEAR(d), 
        MONTH(d),
        CASE MONTH(d) WHEN 1 THEN 'janvier' WHEN 2 THEN 'février' WHEN 3 THEN 'mars'
                        WHEN 4 THEN 'avril' WHEN 5 THEN 'mai' WHEN 6 THEN 'juin'
                        WHEN 7 THEN 'juillet' WHEN 8 THEN 'août' WHEN 9 THEN 'septembre'
                        WHEN 10 THEN 'octobre' WHEN 11 THEN 'novembre' ELSE 'décembre' END,
        DAY(d), 
        js,
        CASE js WHEN 1 THEN 'lundi' WHEN 2 THEN 'mardi' WHEN 3 THEN 'mercredi'
                WHEN 4 THEN 'jeudi' WHEN 5 THEN 'vendredi' WHEN 6 THEN 'samedi' ELSE 'dimanche' END,
        CASE WHEN js >= 6 THEN 1 ELSE 0 END
    FROM jours;
END

GO