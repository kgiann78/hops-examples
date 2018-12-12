# Hops-examples


This is an experimenting repository for geospatial applications to work in hopsworks spark. 

Within the spark directory there are the following applications:

* geospark, that uses txt files as datasets
* stark, that uses tsv files as datasets


In the datasets directory exist the files that are used from the applications, as mentioned above.
GeoSpark is using text files as input datasets with only one geometry per line.
On the other hand, STARK is using tsv files (produced from corresponding shapefiles). Since it contains more information than the geometry itself we use tsv format because a CSV format (with WKT form of the geometry would lead us to several issues).

Note that geometries should not be contained withing double quotes ("") because in that way the applications see the geometries as strings and not actual geometries.

In order to convert a shapefile to a tsv file use the following command: 

    ogr2ogr -f "CSV" -t_srs "WGS84" -overwrite arealm_merge.tsv ~/Downloads/tiger/48_TEXAS/arealm_merge.shp -nlt POLYGON -lco GEOMETRY=AS_WKT -lco SEPARATOR=TAB

That creates a directory named 'arealm_merge.tsv' and inside there is a file named arealm_merge.csv. 

Note, that this not what it seems. If you open the csv file you will notice that it has a properly tsv inside. So, you can rename this file to arealm_merge.tsv (or else) and move it from there. 
