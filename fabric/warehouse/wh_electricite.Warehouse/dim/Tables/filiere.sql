CREATE TABLE [dim].[filiere] (
    [code_filiere] VARCHAR (50) NOT NULL,
    [libellé]      VARCHAR (50) NOT NULL,
    [renouvelable] BIT          NOT NULL,
    [bas_carbone]  BIT          NOT NULL,
    [pilotable]    BIT          NOT NULL
);


GO

ALTER TABLE [dim].[filiere]
    ADD CONSTRAINT [pk_dim_filiere] PRIMARY KEY NONCLUSTERED ([code_filiere] ASC) NOT ENFORCED;


GO