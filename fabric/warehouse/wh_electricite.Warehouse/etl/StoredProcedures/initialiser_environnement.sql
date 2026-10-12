CREATE   PROCEDURE etl.initialiser_environnement AS
BEGIN
    -- C3 : référentiel statique des filières (valeurs à aligner sur dim.filiere de dev)
    INSERT INTO dim.filiere (code_filiere, [libellé], renouvelable, bas_carbone, pilotable)
    SELECT v.code_filiere, v.libelle, v.renouvelable, v.bas_carbone, v.pilotable
    FROM (VALUES
        ('nucleaire',           'Nucléaire',           0, 1, 1),
        ('thermique',           'Thermique fossile',   0, 0, 1),
        ('eolien',              'Éolien',              1, 1, 0),
        ('solaire',             'Solaire',             1, 1, 0),
        ('hydraulique',         'Hydraulique',         1, 1, 1),
        ('bioenergies',         'Bioénergies',         1, 1, 1),
        ('destockage_batterie', 'Déstockage batterie', 0, 0, 1)
    ) AS v (code_filiere, libelle, renouvelable, bas_carbone, pilotable)
    WHERE NOT EXISTS (SELECT 1 FROM dim.filiere AS f WHERE f.code_filiere = v.code_filiere);

    -- C4 : accès complet pour l'identité qui exécute les chargements
    INSERT INTO securite.acces_region (utilisateur, code_region)
    SELECT USER_NAME(), '*'
    WHERE NOT EXISTS (SELECT 1 FROM securite.acces_region
                      WHERE utilisateur = USER_NAME() AND code_region = '*');

    -- C5 : droits du rôle lecteur, absents du projet SQL
    GRANT SELECT ON SCHEMA::fact TO lecteur_regional;
    GRANT SELECT ON dim.[date]   TO lecteur_regional;
    GRANT SELECT ON dim.filiere  TO lecteur_regional;
    GRANT SELECT ON dim.region (libelle_region, code_insee_region, chef_lieu,
                                longitude, latitude, perimetre_eco2mix) TO lecteur_regional;
END

GO