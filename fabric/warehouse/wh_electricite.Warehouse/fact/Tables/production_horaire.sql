CREATE TABLE [fact].[production_horaire] (
    [date_key]     INT           NOT NULL,
    [heure_locale] SMALLINT      NOT NULL,
    [heure_utc]    DATETIME2 (0) NOT NULL,
    [code_region]  VARCHAR (3)   NOT NULL,
    [filiere]      VARCHAR (30)  NOT NULL,
    [mwh]          FLOAT (53)    NULL,
    [nb_pas]       SMALLINT      NOT NULL,
    [annee]        SMALLINT      NOT NULL
);


GO