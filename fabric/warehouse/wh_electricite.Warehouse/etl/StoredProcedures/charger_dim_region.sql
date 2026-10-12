CREATE   PROCEDURE etl.charger_dim_region AS
BEGIN
    DELETE FROM dim.region;
    
    INSERT INTO dim.region
        SELECT 
            libelle_region,
            CAST(code_insee_region AS varchar(3)) AS code_insee_region,
            chef_lieu,
            longitude,
            latitude,
            perimetre_eco2mix,
            (CONCAT('contact.', libelle_region, '@exemple.fr')) AS contact_email
        FROM lh_bronze.dbo.ref_regions reg
        WHERE perimetre_eco2mix = 1
END

GO