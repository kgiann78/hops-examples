package com.examples.hops.spark.geomesa;

import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.geomesa.example.data.GDELTData;
import org.geomesa.example.data.TutorialData;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.Hints;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.fs.FileSystemDataStoreFactory;
import org.locationtech.geomesa.fs.storage.interop.PartitionSchemeUtils;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.sort.SortBy;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class GdeltQueryExample {

    private static Logger log = Logger.getLogger("GdeltQueryExample");


    private final Map<String, String> params;
    private final TutorialData data;
    private final boolean cleanup;
    private final boolean readOnly;

    public GdeltQueryExample(String[] args, DataAccessFactory.Param[] parameters, TutorialData data) throws ParseException {
        this(args, parameters, data, false);
    }

    public GdeltQueryExample(String[] args, DataAccessFactory.Param[] parameters, TutorialData data, boolean readOnly) throws ParseException {
        // parse the data store parameters from the command line
        Properties options = createOptions(parameters);
        Properties commands = CommandLineDataStore.parseArgs(getClass(), options, args);
        params = CommandLineDataStore.getDataStoreParams(commands);
        cleanup = commands.contains("cleanup");
        this.data = data;
        this.readOnly = readOnly;
    }

    public Properties createOptions(DataAccessFactory.Param[] parameters) {
        // parse the data store parameters from the command line
        Properties options = CommandLineDataStore.createOptions(parameters);
        if (!readOnly) {
            options.setProperty("cleanup", "Delete tables after running");
        }
        return options;
    }

    public SimpleFeatureType getSimpleFeatureType(TutorialData data) {


        System.out.println("Get SimpleFeature Type");

        SimpleFeatureType sft = data.getSimpleFeatureType();
        // For the FSDS we need to modify the SimpleFeatureType to specify the index scheme
        org.locationtech.geomesa.fs.storage.api.PartitionScheme scheme = PartitionSchemeUtils.apply(sft, "daily,z2-2bit", Collections.emptyMap());
        PartitionSchemeUtils.addToSft(sft, scheme);

        System.out.println(sft.getTypes());
        return sft;
    }

    public DataStore createDataStore(Map<String, String> params) throws IOException {
        System.out.println("Loading datastore");
        System.out.println(params);
        // use geotools service loading to get a datastore instance
        DataStore datastore = DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        System.out.println();
        return datastore;
    }

    public void cleanup(DataStore datastore, String typeName, boolean cleanup) {
        if (datastore != null) {
            try {
                if (cleanup) {
                    System.out.println("Cleaning up test data");
                    if (datastore instanceof GeoMesaDataStore) {
                        ((GeoMesaDataStore) datastore).delete();
                    } else {
                        ((SimpleFeatureStore) datastore.getFeatureSource(typeName)).removeFeatures(Filter.INCLUDE);
                        datastore.removeSchema(typeName);
                    }
                }
            } catch (Exception e) {
                System.err.println("Exception cleaning up test data: " + e.toString());
            } finally {
                // make sure that we dispose of the datastore when we're done with it
                datastore.dispose();
            }
        }
    }

    public void ensureSchema(DataStore datastore, TutorialData data) throws IOException {
        SimpleFeatureType sft = datastore.getSchema(data.getTypeName());
        if (sft == null) {
            throw new IllegalStateException("Schema '" + data.getTypeName() + "' does not exist. " +
                    "Please run the associated QuickStart to generate the test data.");
        }
    }

    public void createSchema(DataStore datastore, SimpleFeatureType sft) throws IOException {
        System.out.println("Creating schema: " + DataUtilities.encodeType(sft));
        // we only need to do the once - however, calling it repeatedly is a no-op
        datastore.createSchema(sft);
        System.out.println();
    }

    public List<SimpleFeature> getTestFeatures(TutorialData data) {
        System.out.println("Generating test data");
        List<SimpleFeature> features = data.getTestData();
        System.out.println();
        return features;
    }

    public List<Query> getTestQueries(TutorialData data) {
        return data.getTestQueries();
    }

    public void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
        if (features.size() > 0) {
            System.out.println("Writing test data");

            // use try-with-resources to ensure the writer is closed

//            ParquetFileSystemStorage.ParquetFileSystemWriter parquetFileSystemWriter = new ParquetFileSystemStorage.ParquetFileSystemWriter(sft, path, conf);
//            parquetFileSystemWriter.write();

            try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                    datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {

                for (SimpleFeature feature : features) {

                    // using a geotools writer, you have to get a feature, modify it, then commit it
                    // appending writers will always return 'false' for haveNext, so we don't need to bother checking
                    SimpleFeature toWrite = writer.next();

                    // copy attributes
                    toWrite.setAttributes(feature.getAttributes());

                    // if you want to set the feature ID, you have to cast to an implementation class
                    // and add the USE_PROVIDED_FID hint to the user data
                    ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
                    toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                    // alternatively, you can use the PROVIDED_FID hint directly
                    // toWrite.getUserData().put(Hints.PROVIDED_FID, feature.getID());

                    // if no feature ID is set, a UUID will be generated for you

                    // make sure to copy the user data, if there is any
                    toWrite.getUserData().putAll(feature.getUserData());

                    // write the feature
//                    System.out.println(toWrite.getName());
//                    System.out.println(toWrite.getProperties());
//                    System.out.println(toWrite.getDefaultGeometry());
//                    System.out.println(toWrite.getUserData());
//                    System.out.println(toWrite.getAttributes());

                    writer.write();
                }
            }
            System.out.println("Wrote " + features.size() + " features");
            System.out.println();
        }
    }

    public void queryFeatures(DataStore datastore, List<Query> queries) throws IOException {
        for (Query query : queries) {
            System.out.println("Running query " + ECQL.toCQL(query.getFilter()));
            if (query.getPropertyNames() != null) {
                System.out.println("Returning attributes " + Arrays.asList(query.getPropertyNames()));
            }
            if (query.getSortBy() != null) {
                SortBy sort = query.getSortBy()[0];
                System.out.println("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
            }
            // submit the query, and get back an iterator over matching features
            // use try-with-resources to ensure the reader is closed
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                         datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                // loop through all results, only print out the first 10
                int n = 0;
                while (reader.hasNext()) {
                    SimpleFeature feature = reader.next();
                    if (n++ < 10) {
                        // use geotools data utilities to get a printable string
                        System.out.println(String.format("%02d", n) + " " + DataUtilities.encodeFeature(feature));
                    } else if (n == 10) {
                        System.out.println("...");
                    }
                }
                System.out.println();
                System.out.println("Returned " + n + " total features");
                System.out.println();
            }
        }
    }

    public static void main(String[] args) throws IOException, ParseException {

        GdeltQueryExample gdeltQueryExample = new GdeltQueryExample(args, new FileSystemDataStoreFactory().getParametersInfo(), new GDELTData());

        DataStore datastore = null;
        try {


            SparkSession spark = SparkSession
                    .builder()
                    .appName("GdeltQueryExample")
                    .getOrCreate();

            SparkContext sc = spark.sparkContext();

            datastore = gdeltQueryExample.createDataStore(gdeltQueryExample.params);

            if (gdeltQueryExample.readOnly) {
                gdeltQueryExample.ensureSchema(datastore, gdeltQueryExample.data);
            } else {
                SimpleFeatureType sft = gdeltQueryExample.getSimpleFeatureType(gdeltQueryExample.data);

                System.out.println(datastore.getClass().toString());
                gdeltQueryExample.createSchema(datastore, sft);
                List<SimpleFeature> features = gdeltQueryExample.getTestFeatures(gdeltQueryExample.data);
                gdeltQueryExample.writeFeatures(datastore, sft, features);
            }

            List<Query> queries = gdeltQueryExample.getTestQueries(gdeltQueryExample.data);

            gdeltQueryExample.queryFeatures(datastore, queries);


        } catch (Exception e) {
            throw new RuntimeException("Error running quickstart:", e);
        } finally {
            gdeltQueryExample.cleanup(datastore, gdeltQueryExample.data.getTypeName(), gdeltQueryExample.cleanup);
        }
        System.out.println("Done");


//    var n_partitions = 1
//    var n_cores = 8
//
//    if (args.length > 0) {
//      n_partitions = Integer.parseInt(args(0))
//      if (args.length > 1)
//        n_cores = Integer.parseInt(args(1))
//    }
//

        /*
        String predicate = "GdeltQueryExample";
        String path = "hdfs:///Projects/geospatial_queries/Resources/geospatial_results.txt";

        SparkSession spark = SparkSession
                .builder()
                .appName("GdeltQueryExample")
                .getOrCreate();

        SparkContext sc = spark.sparkContext();
        sc.addJar("hdfs:///Projects/geospatial_queries/Resources/geomesa-fs-spark-runtime_2.11-2.2.0-SNAPSHOT.jar");


        Options options = new Options();
        DataAccessFactory.Param[] parameters = {new DataAccessFactory.Param("", )};

        options.

        for (DataAccessFactory.Param p : parameters) {
            if (!p.isDeprecated()) {
                Option opt = Option.builder(null)
                        .longOpt(p.getName())
                        .argName(p.getName())
                        .hasArg()
                        .desc(p.getDescription().toString())
                        .required(p.isRequired())
                        .build();
                options.addOption(opt);
            }
        }

        CommandLine command = parseArgs(getClass(), options, args);
        params = CommandLineDataStore.getDataStoreParams(command, options);


        SimpleFeatureType gdelt = SimpleFeatureTypes.createType("gdelt", "globalEventId:String:index=false,eventCode:String,actor1:String,actor2:String,dtg:Date:index=true,*geom:Point:srid=4326");
        System.out.println(gdelt.getTypeName());

        Map params = new HashMap();

        params.put("fs.path", "/Projects/geospatial_queries/gdelt/2017");
        params.put("geomesa.feature", "gdelt");

        Map conf = new HashMap();
        conf.put("scheme", "gdelt");

        org.locationtech.geomesa.fs.storage.api.PartitionScheme scheme = PartitionScheme.apply(gdelt, "gdelt", conf);
        System.out.println(scheme.getName());

        System.out.println(ConverterStorageFactory.PartitionSchemeParam());
        System.out.println(ConverterStorageFactory.SftConfigParam());


        FileSystemDataStore dataStore = (FileSystemDataStore) DataStoreFinder.getDataStore(params);

        System.out.println(dataStore.getNames());
//
//    println(dataStore)
//    println(dataStore.getSchema)
//
//    //    val dataFrame = spark.read
//    //      .format("geomesa")
//    //      .option("fs.path","/Projects/geospatial_queries/gdelt/2017")
//    //      .option("geomesa.feature", gdelt.getTypeName)
//    ////      .option("geomesa.feature", "gdelt")
//    //      .load()
//    //    dataFrame.createOrReplaceTempView("gdelt")
//    //
//    //    // Select the top event codes
//    //    spark.sql("SELECT eventCode, count(*) as count FROM gdelt " +
//    //      "WHERE dtg >= cast('2017-06-01T00:00:00.000' as timestamp) " +
//    //      "AND dtg <= cast('2017-12-31T00:00:00.000' as timestamp) " +
//    //      "GROUP BY eventcode ORDER by count DESC").show()
//
//
//    val writer = new HDFSWriter(path)
//    var exception: Throwable = null
//
//    try {
//      writer.write("GeoMesa", predicate, false, n_partitions, n_cores, 0d, 0d, 0l)
//    } catch {
//      case NonFatal(e) => {
//        exception = e
//        throw e
//      }
//    } finally {
//      Utils.closeAndSuppressed(exception, writer)
//      writer.close()
//    }
//
*/
    }


    public Map<String, String> getParams() {
        return params;
    }

    public TutorialData getData() {
        return data;
    }

    public boolean isCleanup() {
        return cleanup;
    }

    public boolean isReadOnly() {
        return readOnly;
    }
}
