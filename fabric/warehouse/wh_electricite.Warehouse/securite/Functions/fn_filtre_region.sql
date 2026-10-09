CREATE FUNCTION securite.fn_filtre_region (@code_region varchar(3))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS acces_autorise
    FROM securite.acces_region AS a
    WHERE a.utilisateur = USER_NAME()
      AND (a.code_region = @code_region OR a.code_region = '*');

GO