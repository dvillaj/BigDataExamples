#!/bin/bash

BASEDIR=$(dirname $0)
cd $BASEDIR

/home/cloudera/GetDataWS/get_data.sh \
    --dir data/precios \
    --item ListaEESSPrecio \
    --timestamp \
    --clean \
    https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/
