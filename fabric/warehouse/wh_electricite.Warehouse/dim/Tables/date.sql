CREATE TABLE [dim].[date] (
    [date_key]     INT          NOT NULL,
    [date]         DATE         NOT NULL,
    [annee]        SMALLINT     NOT NULL,
    [mois]         SMALLINT     NOT NULL,
    [mois_libelle] VARCHAR (10) NOT NULL,
    [jour]         SMALLINT     NOT NULL,
    [jour_semaine] SMALLINT     NOT NULL,
    [jour_libelle] VARCHAR (10) NOT NULL,
    [est_weekend]  BIT          NOT NULL
);


GO

ALTER TABLE [dim].[date]
    ADD CONSTRAINT [pk_dim_date] PRIMARY KEY NONCLUSTERED ([date_key] ASC) NOT ENFORCED;


GO