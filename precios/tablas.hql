DROP DATABASE IF EXISTS precios CASCADE;

CREATE DATABASE precios;

DROP TABLE precios.precio_combustible_json;

CREATE EXTERNAL TABLE precios.precio_combustible_json (
    `provincia` STRING,
    `idprovincia` STRING,
    `idccaa` STRING, 
    `precio_gasoleo_a` STRING,
    `precio_nuevo_gasoleo_a` STRING,
    `idmunicipio` STRING,
    `ideess` STRING,
    `precio_gasolina__98` STRING,
    `municipio` STRING,
    `c_p_` STRING,
    `r_tulo`  STRING,
    `direcci_n` STRING,
    `localidad` STRING,
    `remisi_n` STRING,
    `margen` STRING,
    `_timestamp` STRING
)
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDePlus' 
LOCATION '/raw/precios'; 
;

CREATE EXTERNAL TABLE precios.ComunidadesAutonomas ( 
    CCAA STRING, 
    IDCCAA STRING
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/raw/ccaa';


CREATE OR REPLACE VIEW precios.vi_precio_combustible as 
SELECT
    p.ideess as id_eess,
    p.idccaa as id_ccaa, 
    p.provincia,
    p.idprovincia as id_provincia,
    p.municipio as municipio,
    p.idmunicipio as id_municipio,
    p.c_p_ as cod_postal,
    upper(p.r_tulo)  as rotulo,
    p.direcci_n as direccion,
    p.localidad as localidad,
    upper(p.remisi_n) as remision,
    p.margen as margen,
    cast(regexp_replace(p.precio_gasoleo_a, ',', '.') as decimal(5,3)) as precio_diesel,
    cast(regexp_replace(p.precio_nuevo_gasoleo_a, ',', '.') as decimal(5,3)) as precio_diesel_plus,
    cast(regexp_replace(p.precio_gasolina__98, ',', '.') as decimal(5,3)) as precio_gasolina,
    cast(p.`_timestamp` as timestamp) as fecha_descarga
FROM precios.precio_combustible_json p
;
