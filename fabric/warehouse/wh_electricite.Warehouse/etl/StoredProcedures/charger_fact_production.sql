CREATE   PROCEDURE etl.charger_fact_production @annee_debut smallint, @run_id varchar(64)
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        DELETE FROM fact.production_horaire WHERE annee >= @annee_debut;

        INSERT INTO fact.production_horaire
        SELECT YEAR(heure_paris)*10000 + MONTH(heure_paris)*100 + DAY(heure_paris),
               DATEPART(hour, heure_paris), heure_utc, code_region, filiere, mwh, nb_pas, annee
        FROM lh_gold.dbo.fact_production_horaire
        WHERE annee >= @annee_debut;

        -- contrôles
        DECLARE @attendu bigint = (SELECT MAX(lignes) FROM lh_admin.dbo.log_execution
                                    WHERE run_id = @run_id AND table_cible = 'fact_production_horaire'
                                        AND statut = 'succes');
        DECLARE @charge  bigint = (SELECT COUNT_BIG(*) FROM fact.production_horaire WHERE annee >= @annee_debut);

        IF @attendu IS NULL
            THROW 50001, 'Aucune écriture gold journalisée pour ce run (endpoint pas encore synchronisé ?)', 1;
        IF @attendu <> @charge
            THROW 50002, 'Le nombre de lignes chargées diffère de celui écrit par nb_gold', 1;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;      -- relance l'erreur : le pipeline doit voir l'échec, comme le raise de publier
    END CATCH
END

GO