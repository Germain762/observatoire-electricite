[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared regions = let
  Source = Json.Document(
    Web.Contents("https://geo.api.gouv.fr", [RelativePath = "regions", Query = [zone = "metro"]]),
    65001
),
  #"Converti en table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Column1 développé" = Table.ExpandRecordColumn(#"Converti en table", "Column1", {"nom", "code"}, {"nom", "code"}),
  #"Colonnes renommées" = Table.RenameColumns(#"Column1 développé", {{"nom", "libelle_region"}, {"code", "code_insee_region"}}),
  #"Type de colonne changé" = Table.TransformColumnTypes(#"Colonnes renommées", {{"libelle_region", type text}, {"code_insee_region", type text}})
in
  #"Type de colonne changé";
shared chefs_lieux = let
  Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("LZBBjgIhEEXvwtoFUBQ0S80sdcbo7DouGCWT7nQgoSVxjjTn8GJWgavKfz/Fr884CqXERhxDmVaaDiVacdmMQhuSX2V5/ofEjkENpjuO5Mc050RTKw2q44HkKdfI2Fk0smHQJPfTskSa6AE7Nvz8+V7C+pNr+SVhnRl085BXPkO6xxZslPSdAyfElBoHOmho3CHJXS63GOqDHZAWumNJfue65LpyPihEbM7A+fu/1sF6KtGo54RDKGt8H6xAvhc8L2zncL1OmXtvpaTvuLwA", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [code_region = _t, chef_lieu = _t, code_commune = _t]),
  #"Fonction personnalisée appelée" = Table.AddColumn(Source, "centre", each fn_centre_commune([code_commune]), type record),
  #"centre développé 1" = Table.ExpandRecordColumn(#"Fonction personnalisée appelée", "centre", {"longitude", "latitude"}, {"longitude", "latitude"}),
  #"Type de colonne changé" = Table.TransformColumnTypes(#"centre développé 1", {{"longitude", type number}, {"code_region", type text}, {"chef_lieu", type text}, {"code_commune", type text}, {"latitude", type number}})
in
  #"Type de colonne changé";
shared fn_centre_commune = (code_commune as text) as record =>
let
    Source = Json.Document(
        Web.Contents(
            "https://geo.api.gouv.fr",
            [
                RelativePath = "communes/" & code_commune,
                Query = [fields = "nom,centre"]
            ]
        )
    ),
    coords = Source[centre][coordinates]
in
    [longitude = coords{0}, latitude = coords{1}];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Fusionner_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared ref_regions = let
  Source = Table.NestedJoin(regions, {"code_insee_region"}, chefs_lieux, {"code_region"}, "chefs_lieux", JoinKind.Inner),
  #"chefs_lieux développé" = Table.ExpandTableColumn(Source, "chefs_lieux", {"code_region", "chef_lieu", "code_commune", "longitude", "latitude"}, {"code_region", "chef_lieu", "code_commune", "longitude", "latitude"}),
  #"Colonnes supprimées" = Table.RemoveColumns(#"chefs_lieux développé", {"code_region", "code_commune"})
in
  #"Colonnes supprimées";
shared Fusionner_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "ad90f025-9a04-41a2-b271-9b5881e5e6ad"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "5cfb5a02-b6fb-48e7-8e22-3cb56bbb5daf"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "ref_regions", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
