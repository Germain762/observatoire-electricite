CREATE   PROCEDURE etl.charger_dim_region AS
BEGIN
    DELETE FROM dim.region;
    
    INSERT INTO dim.region
        SELECT 
            reg.*,
            (CONCAT('contact.', libelle_region, '@exemple.fr')) AS contact_email
        FROM lh_bronze.dbo.ref_regions reg
        WHERE perimetre_eco2mix = 1
END

GO