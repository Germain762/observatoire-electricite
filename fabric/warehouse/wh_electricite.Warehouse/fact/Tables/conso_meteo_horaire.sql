CREATE TABLE [fact].[conso_meteo_horaire] (
    [date_key]        INT           NOT NULL,
    [heure_locale]    SMALLINT      NOT NULL,
    [heure_utc]       DATETIME2 (0) NOT NULL,
    [code_region]     VARCHAR (3)   NOT NULL,
    [conso_mwh]       FLOAT (53)    NULL,
    [temperature_c]   FLOAT (53)    NULL,
    [vent_kmh]        FLOAT (53)    NULL,
    [rayonnement_wm2] FLOAT (53)    NULL,
    [nb_pas]          SMALLINT      NOT NULL,
    [annee]           SMALLINT      NOT NULL
);


GO