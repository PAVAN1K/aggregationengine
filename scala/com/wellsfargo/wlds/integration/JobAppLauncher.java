package com.wellsfargo.wlds.integration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.util.Map;

import static java.lang.System.exit;
import static java.lang.Thread.sleep;

public class JobAppLauncher {

    private static String SPARK_MASTER = "SPARK_MASTER";
    private static String DEPLOY_MODE = "DEPLOY_MODE";
    private static String SPARK_HOME = "SPARK_HOME";
    private static String SPARK_SQL_WAREHOUSE_DIR = "SPARK_SQL_WAREHOUSE_DIR";
    private static String HIVE_SITE_FILE = "HIVE_SITE_FILE";
    private static String SET_VERBOSE = "SET_VERBOSE";
    private static String APP_RESOURCE = "APP_RESOURCE";
    private static String MAINCLASS = "MAINCLASS";
    private static String QUEUE_NAME = "QUEUE_NAME";
    private static String DRIVER_MEMORY = "DRIVER_MEMORY";
    private static String EXECUTOR_MEMORY = "EXECUTOR_MEMORY";
    private static String EXECUTOR_CORES = "EXECUTOR_CORES";
    private static String EXECUTOR_INSTANCES = "EXECUTOR_INSTANCES";
    private static String CONF = "CONF";

    public static void main(String[] args) throws Exception {

        if (args.length < 7) {
            System.out.println("Insufficient arguments.");
            System.out.println("Args List: <Aggregation_MainClass>, <AggregationEngine_Jar_Path>, <config_file_Path>," +
                    " <app_name>, <env>");
            exit(-1);
        }
        String appArgs[] = new String[]{args[3], args[4], args[5]};

        File file = new File(args[0]);
        Config config = ConfigFactory.load(ConfigFactory.parseFile(file));
        System.out.println("Config File: " + config);

        SparkLauncher sparkLauncher = new SparkLauncher();

        if (config.hasPath(SPARK_MASTER)) {
            sparkLauncher.setMaster(config.getString(SPARK_MASTER));
        }
        if (config.hasPath(DEPLOY_MODE)) {
            sparkLauncher.setDeployMode(config.getString(DEPLOY_MODE));
        }
        if (config.hasPath(SPARK_HOME)) {
            sparkLauncher.setSparkHome(config.getString(SPARK_HOME));
        }
        if (config.hasPath(SPARK_SQL_WAREHOUSE_DIR)) {
            sparkLauncher.setConf("spark.sql.warehouse.dir", config.getString(SPARK_SQL_WAREHOUSE_DIR));
        }
        if (config.hasPath(HIVE_SITE_FILE)) {
            sparkLauncher.addFile(config.getString(HIVE_SITE_FILE));
        }
        if (config.hasPath(SET_VERBOSE)) {
            sparkLauncher.setVerbose(config.getBoolean(SET_VERBOSE));
        }
        if (config.hasPath(DRIVER_MEMORY)) {
            sparkLauncher.setConf(SparkLauncher.DRIVER_MEMORY, config.getString(DRIVER_MEMORY));
        }
        if (config.hasPath(EXECUTOR_MEMORY)) {
            sparkLauncher.setConf(SparkLauncher.EXECUTOR_MEMORY, config.getString(EXECUTOR_MEMORY));
        }
        if (config.hasPath(EXECUTOR_CORES)) {
            sparkLauncher.setConf(SparkLauncher.EXECUTOR_CORES, config.getString(EXECUTOR_CORES));
        }
        if (config.hasPath(EXECUTOR_INSTANCES)) {
            sparkLauncher.setConf("spark.executor.instances", config.getString(EXECUTOR_INSTANCES));
        }
        if (config.hasPath(QUEUE_NAME)) {
            sparkLauncher.setConf("spark.yarn.queue", config.getString(QUEUE_NAME));
        }


        sparkLauncher.setAppName(args[4]);
        sparkLauncher.setConf("spark.sql.hive.metastore.jars", "builtin");
        sparkLauncher.setConf("spark.sql.hive.convertMetastoreParquet", "false");
        sparkLauncher.setConf("spark.sql.hive.caseSensitiveInferenceMode", "NEVER_INFER");
        sparkLauncher.setConf("spark.sql.crossJoin.enabled", "true");

        for (int i = 0; i < args[6].split(",").length; i++) {
            System.out.println("added Jars: " + args[6].split(",")[i]);
            sparkLauncher.addJar(args[6].split(",")[i]);
        }

        if (config.hasPath(CONF)) {
            for (Map.Entry<String, ConfigValue> entry : config.getConfig(CONF).entrySet()) {
                sparkLauncher.setConf(entry.getKey().replaceAll("_", "."), entry.getValue().unwrapped().toString());
                System.out.println(entry.getKey().replaceAll("_", ".") + ":" + entry.getValue().unwrapped().toString());
            }
        }

        SparkAppHandle handle = sparkLauncher.setMainClass(args[1])
                .setAppResource(args[2])
                .addAppArgs(appArgs)
                .startApplication();
        SparkAppHandle.State final_state;
        long start = System.currentTimeMillis();
        for (; ; ) {

            sleep(10000);
            double elapsed = Math.round((System.currentTimeMillis() - start) / 60000);
            System.out.println("Elapsed Minutes " + elapsed + ":" + handle.getAppId() + "," + handle.getState());
            if (handle.getState() == SparkAppHandle.State.FAILED || handle.getState() == SparkAppHandle.State.KILLED || handle.getState() == SparkAppHandle.State.FINISHED) {
                final_state = handle.getState();
                System.err.println("FINAL STATE=" + final_state);
                break;
            }
        }

        exit(final_state == SparkAppHandle.State.FINISHED ? 0 : -1);

    }
}