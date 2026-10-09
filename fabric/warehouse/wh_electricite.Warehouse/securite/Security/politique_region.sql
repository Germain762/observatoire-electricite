CREATE SECURITY POLICY [securite].[politique_region]
    ADD FILTER PREDICATE [securite].[fn_filtre_region]([code_region]) ON [fact].[production_horaire],
    ADD FILTER PREDICATE [securite].[fn_filtre_region]([code_region]) ON [fact].[conso_meteo_horaire],
    ADD FILTER PREDICATE [securite].[fn_filtre_region]([code_insee_region]) ON [dim].[region]
    WITH (STATE = ON);


GO