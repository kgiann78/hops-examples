# Hops-examples


This is an experimenting repository for geospatial applications to work in hopsworks spark. 


    ogr2ogr -f "CSV" -t_srs "WGS84" -overwrite arealm_merge.tsv ~/Downloads/tiger/48_TEXAS/arealm_merge.shp -nlt POLYGON -lco GEOMETRY=AS_WKT -lco SEPARATOR=TAB
