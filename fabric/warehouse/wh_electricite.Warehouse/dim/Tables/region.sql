CREATE TABLE [dim].[region] (
    [libelle_region]    VARCHAR (50)                                     NOT NULL,
    [code_insee_region] VARCHAR (50)                                     NOT NULL,
    [chef_lieu]         VARCHAR (50)                                     NOT NULL,
    [longitude]         FLOAT (53)                                       NOT NULL,
    [latitude]          FLOAT (53)                                       NOT NULL,
    [perimetre_eco2mix] BIT                                              NOT NULL,
    [contact_email]     VARCHAR (100) MASKED WITH (FUNCTION = 'email()') NOT NULL
);


GO