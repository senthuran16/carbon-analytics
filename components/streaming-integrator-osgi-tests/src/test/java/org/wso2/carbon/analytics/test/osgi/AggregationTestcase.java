/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.carbon.analytics.test.osgi;

import com.zaxxer.hikari.HikariDataSource;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.aggregation.Executor;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.query.api.aggregation.TimePeriod;
import org.apache.log4j.Logger;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.ExamFactory;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.ops4j.pax.exam.testng.listener.PaxExam;
import org.osgi.framework.BundleContext;
import org.testng.Assert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import org.wso2.carbon.analytics.test.osgi.util.RDBMSConfig;
import org.wso2.carbon.container.CarbonContainerFactory;
import org.wso2.carbon.container.options.CarbonDistributionOption;
import org.wso2.carbon.datasource.core.api.DataSourceService;
import org.wso2.carbon.datasource.core.exception.DataSourceException;
import org.wso2.carbon.kernel.CarbonServerInfo;
import org.wso2.carbon.streaming.integrator.core.internal.StreamProcessorDataHolder;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.wso2.carbon.container.options.CarbonDistributionOption.carbonDistribution;
import static org.wso2.carbon.container.options.CarbonDistributionOption.copyFile;

@Listeners(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
@ExamFactory(CarbonContainerFactory.class)
public class AggregationTestcase {

    private class AssertionInfoHolder {
        List<Object[]> events = new ArrayList<>();
        AtomicInteger eventCount = new AtomicInteger(0);
        boolean eventArrived = false;
    }

    @Inject
    protected BundleContext bundleContext;

    @Inject
    private CarbonServerInfo carbonServerInfo;

    @Inject
    private DataSourceService dataSourceService;

//    @Inject
//    private AggregationParser aggregationParser;

    private static final Logger log = Logger.getLogger(AggregationTestcase.class);
    private static final String CARBON_YAML_FILENAME = "deployment.yaml";
    private static final String TABLE_NAME = "PERSISTENCE_TABLE";
    private static final String SIDDHIAPP_NAME = "SiddhiAppPersistence";
//    private static final String OJDBC6_OSGI_DEPENDENCY = "ojdbc6_12.1.0.1_atlassian_hosted_1.0.0.jar";
    private static final String OJDBC6_OSGI_DEPENDENCY = "ojdbc8_12.2.0.1_1.0.0.jar";
    private static final String MYSQL_OSGI_DEPENDENCY = "mysql_connector_java_5.1.45_1.0.0.jar";

    private final String selectLastQuery = "SELECT siddhiAppName FROM " + TABLE_NAME + " WHERE siddhiAppName = ?";

    /**
     * Replace the existing deployment.yaml file with populated deployment.yaml file.
     */
    private Option copyCarbonYAMLOption() {
        Path carbonYmlFilePath;
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = Paths.get(".").toString();
        }
        carbonYmlFilePath = Paths.get(basedir, "src", "test", "resources",
                "conf", "persistence", "db", CARBON_YAML_FILENAME);
        return copyFile(carbonYmlFilePath, Paths.get("conf", "server", CARBON_YAML_FILENAME));
    }

    /**
     * Copy the OJDBC OSGI dependency to lib folder of distribution
     */
    private Option copyOracleJDBCJar() {
        Path ojdbc6FilePath;
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = Paths.get(".").toString();
        }
        ojdbc6FilePath = Paths.get(basedir, "src", "test", "resources", "lib", OJDBC6_OSGI_DEPENDENCY);
        return copyFile(ojdbc6FilePath, Paths.get("lib", OJDBC6_OSGI_DEPENDENCY));
    }

    private Option copyMySQLJDBCJar() {
        Path ojdbc6FilePath;
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = Paths.get(".").toString();
        }
        ojdbc6FilePath = Paths.get(basedir, "src", "test", "resources", "lib", MYSQL_OSGI_DEPENDENCY);
        return copyFile(ojdbc6FilePath, Paths.get("lib", MYSQL_OSGI_DEPENDENCY));
    }

    @Configuration
    public Option[] createConfiguration() {
        log.info("Running - " + this.getClass().getName());
        RDBMSConfig.createDatasource("db");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            log.error("Error in waiting for DataSources configuration file creation");
        }
        return new Option[]{
                CarbonDistributionOption.debug(5005),
                copyCarbonYAMLOption(),
                copyOracleJDBCJar(),
                copyMySQLJDBCJar(),
//                CarbonDistributionOption.copyOSGiLibBundle(maven(
//                        "mysql", "mysql-connector-java").versionAsInProject()),
//                CarbonDistributionOption.copyOSGiLibBundle(maven(
//                        "org.postgresql", "postgresql").versionAsInProject()),
//                CarbonDistributionOption.copyOSGiLibBundle(maven(
//                        "com.microsoft.sqlserver", "mssql-jdbc").versionAsInProject()),
                carbonDistribution(Paths.get("target", "wso2-streaming-integrator-" +
                                System.getProperty("carbon.analytic.version")), "server")
        };
    }

    // TODO agg test

    private String getStoreAnnotation() {
        return "@Store(type=\"rdbms\", datasource=\"WSO2_ANALYTICS_DB\"" + ")\n" +
            "@purge(enable='false')\n" +
//            "@persistedAggregation(enable='true')\n" +
            "";
    }

    private void addCallback(String queryName, SiddhiAppRuntime siddhiAppRuntime, AssertionInfoHolder holder) {
        siddhiAppRuntime.addCallback(queryName, new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        holder.events.add(event.getData());
                        holder.eventCount.incrementAndGet();
                    }
                    holder.eventArrived = true;
                }
            }
        });
    }

//    private void executeHoursAggregationDurationExecutor(TimePeriod.Duration duration) {
////        try {
//            Map<String, Map<TimePeriod.Duration, Executor>> aggDurationExecutorMap = AggregationParser.getAggregationDurationExecutorMap();
//            ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>();
//            StreamEvent streamEvent = new StreamEvent(0, 0,0);
//            streamEvent.setType(ComplexEvent.Type.TIMER);
//            streamEvent.setTimestamp(HybridAggregationTestUtils.convertToEpoch("2021-01-01 01:00:01 GMT+0:00"));
//            complexEventChunk.add(streamEvent);
//            aggDurationExecutorMap.get("REQUEST_SUMMARY").get(duration).execute(complexEventChunk);
////        } catch (Throwable throwable) {
////            log.error(">>>>>>>>>>>>>>", throwable);
////        }
//    }

    private void executeAggregationDurationExecutor(SiddhiAppRuntime siddhiAppRuntime,
                                                    TimePeriod.Duration duration) {
//        try {
//        Map<String, Map<TimePeriod.Duration, Executor>> aggDurationExecutorMap = AggregationParser.getAggregationDurationExecutorMap();
        log.info(">>>>>>>>>>>>>>>" + siddhiAppRuntime.getAggregationMap().keySet().toArray().toString());
        Map<String, Map<TimePeriod.Duration, Executor>> aggDurationExecutorMap = siddhiAppRuntime.getAggregationMap().get("REQUEST_SUMMARY").getAggregationDurationExecutorMap();
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>();
        StreamEvent streamEvent = new StreamEvent(0, 0,0);
        streamEvent.setType(ComplexEvent.Type.TIMER);

        // todo
//        long currentTime = System.currentTimeMillis();


        streamEvent.setTimestamp(HybridAggregationTestUtils.convertToEpoch("2021-01-01 01:00:01 GMT+0:00"));
        complexEventChunk.add(streamEvent);
        aggDurationExecutorMap.get("REQUEST_SUMMARY").get(duration).execute(complexEventChunk);
//        } catch (Throwable throwable) {
//            log.error(">>>>>>>>>>>>>>", throwable);
//        }
    }

    private void executeAggregationDurationExecutor(SiddhiAppRuntime siddhiAppRuntime,
                                                    TimePeriod.Duration duration,
                                                    long timestamp) {
        log.info(">>>>>>>>>>>>>>>" + siddhiAppRuntime.getAggregationMap().keySet().toArray().toString());
        Map<String, Map<TimePeriod.Duration, Executor>> aggDurationExecutorMap = siddhiAppRuntime.getAggregationMap().get("REQUEST_SUMMARY").getAggregationDurationExecutorMap();
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>();
        StreamEvent streamEvent = new StreamEvent(0, 0,0);
        streamEvent.setType(ComplexEvent.Type.TIMER);
        streamEvent.setTimestamp(HybridAggregationTestUtils.convertToEpoch("2021-01-01 01:00:01 GMT+0:00"));
        complexEventChunk.add(streamEvent);
        aggDurationExecutorMap.get("REQUEST_SUMMARY").get(duration).execute(complexEventChunk);
    }

    private List<Object[]> runStatementAndGetResult(String query) throws DataSourceException, SQLException {
        DataSource dataSource = (HikariDataSource) dataSourceService.getDataSource("WSO2_ANALYTICS_DB");
        PreparedStatement stmt = null;
        Connection con = null;
        List<Object[]> events = new ArrayList<>(0);
        try {
            con = dataSource.getConnection();
            stmt = con.prepareStatement(query);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                long timestamp = resultSet.getLong("AGG_EVENT_TIMESTAMP");
                long totalRequestCount = resultSet.getLong("TOTALREQUESTCOUNT");
                double avgWaitingTime = resultSet.getDouble("AVGWAITINGTIME");
                double totalWaitingTime = resultSet.getDouble("TOTALWAITINGTIME");
                double minWaitingTime = resultSet.getDouble("MINWAITINGTIME");
                double maxWaitingTime = resultSet.getDouble("MAXWAITINGTIME");
                events.add(new Object[]{timestamp, totalRequestCount, avgWaitingTime, totalWaitingTime, minWaitingTime, maxWaitingTime});
            }
            return events;
        } catch (SQLException e) {
            log.error("Getting result from DB table failed due to " + e.getMessage(), e);
            return events;
        } finally {
            con.close();
            stmt.close();
        }
    }

    private void assertTimestamps(String granularity) throws DataSourceException, SQLException {
        DataSource dataSource = (HikariDataSource) dataSourceService.getDataSource("WSO2_ANALYTICS_DB");
        String query = generateRawAssertQuery(granularity);
        PreparedStatement stmt = null;
        Connection con = null;
        try {
            con = dataSource.getConnection();
            stmt = con.prepareStatement(query);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                long aggTimestamp = resultSet.getLong("AGG_TIMESTAMP");
                long aggEventTimestamp = resultSet.getLong("AGG_EVENT_TIMESTAMP");
                long aggLastEventTimestamp = resultSet.getLong("AGG_LAST_EVENT_TIMESTAMP");
                Assert.assertTrue(aggTimestamp >= aggEventTimestamp, "suspect");
            }
        } catch (SQLException e) {
            log.error("Getting result from DB table failed due to " + e.getMessage(), e);
        } finally {
            con.close();
            stmt.close();
        }
    }

    private String generateAggregatedAssertQuery(String granularity) {
        return "select AGG_EVENT_TIMESTAMP, sum(AGG_COUNT) as totalRequestCount, (sum(AGG_SUM_WAITINGTIME)/sum(AGG_COUNT)) as avgWaitingTime, sum(AGG_SUM_WAITINGTIME) as totalWaitingTime, min(AGG_MIN_WAITINGTIME) as minWaitingTime, max(AGG_MAX_WAITINGTIME) as maxWaitingTime from wso2.REQUEST_SUMMARY_" + granularity + " group by AGG_EVENT_TIMESTAMP, context";
    }

    private String generateAggregatedAssertQuery2(String granularity) {
        return "select AGG_EVENT_TIMESTAMP, sum(AGG_COUNT) as totalRequestCount, (sum(AGG_SUM_WAITINGTIME)/sum(AGG_COUNT)) as avgWaitingTime, sum(AGG_SUM_WAITINGTIME) as totalWaitingTime, min(AGG_MIN_WAITINGTIME) as minWaitingTime, max(AGG_MAX_WAITINGTIME) as maxWaitingTime from wso2.REQUEST_SUMMARY_" + granularity + " group by AGG_EVENT_TIMESTAMP, context, version";
    }

    private String generateRawAssertQuery(String granularity) {
        // eg: YEARS
        return "select * from wso2.REQUEST_SUMMARY_" + granularity; // TODO wso2. part is hardcoded.
    }

    // Starting from sec [START]

//    @Test // TODO Passes
    public void testContinuousSecToMin() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousSecToMin");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertMinutesStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    count() as totalRequestCount,\n" +
                        "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                        "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every sec...min;\n" +


                        /* Per minutes */
                        // Group by: context
                        "\n@info(name = 'assertMinutes')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMinutes2')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert minutes
            AssertionInfoHolder assertMinutesInfo = new AssertionInfoHolder();
            addCallback("assertMinutes", siddhiAppRuntime, assertMinutesInfo);
            AssertionInfoHolder assertMinutes2Info = new AssertionInfoHolder();
            addCallback("assertMinutes2", siddhiAppRuntime, assertMinutes2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertMinutesStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMinutesStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.MINUTES);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertMinutesStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events
            List<Object[]> expectedMinutes = Arrays.asList(
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591074060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984060000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedMinutes2 = Arrays.asList(
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530417660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527915660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590984060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0});

            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 32, assertMinutesInfo.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertMinutes2Info.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutesInfo.events, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutes2Info.events, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> minutesRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MINUTES"));
            List<Object[]> minutes2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MINUTES"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutesRecords, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutes2Records, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("SECONDS");
            assertTimestamps("MINUTES");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

//    @Test // TODO Passes
    public void testContinuousSecToHour() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousSecToHour");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertMinutesStream(startTime string, endTime string);\n" +
                        "define stream AssertHoursStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    count() as totalRequestCount,\n" +
                        "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                        "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every sec...hour;\n" +


                        /* Per minutes */
                        // Group by: context
                        "\n@info(name = 'assertMinutes')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMinutes2')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per hours */
                        // Group by: context
                        "\n@info(name = 'assertHours')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertHours2')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert minutes
            AssertionInfoHolder assertMinutesInfo = new AssertionInfoHolder();
            addCallback("assertMinutes", siddhiAppRuntime, assertMinutesInfo);
            AssertionInfoHolder assertMinutes2Info = new AssertionInfoHolder();
            addCallback("assertMinutes2", siddhiAppRuntime, assertMinutes2Info);

            // Info to assert hours
            AssertionInfoHolder assertHoursInfo = new AssertionInfoHolder();
            addCallback("assertHours", siddhiAppRuntime, assertHoursInfo);
            AssertionInfoHolder assertHours2Info = new AssertionInfoHolder();
            addCallback("assertHours2", siddhiAppRuntime, assertHours2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertMinutesStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMinutesStream");
            InputHandler assertHoursStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertHoursStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.MINUTES);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertMinutesStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertHoursStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events
            List<Object[]> expectedMinutes = Arrays.asList(
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591074060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984060000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedMinutes2 = Arrays.asList(
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530417660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527915660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590984060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0});

            List<Object[]> expectedHours = Arrays.asList(
                    new Object[]{1591074000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593666000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590984000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527915600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591070400000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530417600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedHours2 = Arrays.asList(
                    new Object[]{1591074000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593576000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530504000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527915600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527829200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593666000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1590987600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527912000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 3.0,3.0,3.0,3.0});

            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 32, assertMinutesInfo.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertMinutes2Info.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 32, assertHoursInfo.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertHours2Info.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutesInfo.events, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutes2Info.events, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHoursInfo.events, expectedHours), "hours - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHours2Info.events, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> minutesRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MINUTES"));
            List<Object[]> minutes2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MINUTES"));
            List<Object[]> hoursRecords = runStatementAndGetResult(generateAggregatedAssertQuery("HOURS"));
            List<Object[]> hours2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("HOURS"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutesRecords, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutes2Records, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hoursRecords, expectedHours), "hours - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hours2Records, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("SECONDS");
            assertTimestamps("MINUTES");
            assertTimestamps("HOURS");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

//    @Test // TODO Passes
    public void testContinuousSecToDay() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousSecToDay");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertMinutesStream(startTime string, endTime string);\n" +
                        "define stream AssertHoursStream(startTime string, endTime string);\n" +
                        "define stream AssertDaysStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    count() as totalRequestCount,\n" +
                        "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                        "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every sec...day;\n" +


                        /* Per minutes */
                        // Group by: context
                        "\n@info(name = 'assertMinutes')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMinutes2')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per hours */
                        // Group by: context
                        "\n@info(name = 'assertHours')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertHours2')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per days */
                        // Group by: context
                        "\n@info(name = 'assertDays')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertDays2')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert minutes
            AssertionInfoHolder assertMinutesInfo = new AssertionInfoHolder();
            addCallback("assertMinutes", siddhiAppRuntime, assertMinutesInfo);
            AssertionInfoHolder assertMinutes2Info = new AssertionInfoHolder();
            addCallback("assertMinutes2", siddhiAppRuntime, assertMinutes2Info);

            // Info to assert hours
            AssertionInfoHolder assertHoursInfo = new AssertionInfoHolder();
            addCallback("assertHours", siddhiAppRuntime, assertHoursInfo);
            AssertionInfoHolder assertHours2Info = new AssertionInfoHolder();
            addCallback("assertHours2", siddhiAppRuntime, assertHours2Info);

            // Info to assert days
            AssertionInfoHolder assertDaysInfo = new AssertionInfoHolder();
            addCallback("assertDays", siddhiAppRuntime, assertDaysInfo);
            AssertionInfoHolder assertDays2Info = new AssertionInfoHolder();
            addCallback("assertDays2", siddhiAppRuntime, assertDays2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertMinutesStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMinutesStream");
            InputHandler assertHoursStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertHoursStream");
            InputHandler assertDaysStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertDaysStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.MINUTES);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertMinutesStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertHoursStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertDaysStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events
            List<Object[]> expectedMinutes = Arrays.asList(
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591074060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984060000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedMinutes2 = Arrays.asList(
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530417660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527915660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590984060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0});

            List<Object[]> expectedHours = Arrays.asList(
                    new Object[]{1591074000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593666000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590984000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527915600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591070400000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530417600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedHours2 = Arrays.asList(
                    new Object[]{1591074000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593576000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530504000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527915600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527829200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593666000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1590987600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527912000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 3.0,3.0,3.0,3.0});

            List<Object[]> expectedDays = Arrays.asList(
                    new Object[]{1590969600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1530489600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593648000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1590969600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593561600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530489600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593648000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 1.5,6.0,1.0,2.0});

            List<Object[]> expectedDays2 = Arrays.asList(
                    new Object[]{1590969600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530489600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527811200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593648000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593561600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1527897600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593561600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527811200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530489600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593561600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1530489600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1591056000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1593561600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527897600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1590969600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527897600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593648000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1530489600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527897600000L, 2L, 4.0,8.0,4.0,4.0});


            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 32, assertMinutesInfo.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertMinutes2Info.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 32, assertHoursInfo.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertHours2Info.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 16, assertDaysInfo.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 32, assertDays2Info.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutesInfo.events, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutes2Info.events, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHoursInfo.events, expectedHours), "hours - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHours2Info.events, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDaysInfo.events, expectedDays), "days - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDays2Info.events, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> minutesRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MINUTES"));
            List<Object[]> minutes2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MINUTES"));
            List<Object[]> hoursRecords = runStatementAndGetResult(generateAggregatedAssertQuery("HOURS"));
            List<Object[]> hours2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("HOURS"));
            List<Object[]> daysRecords = runStatementAndGetResult(generateAggregatedAssertQuery("DAYS"));
            List<Object[]> days2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("DAYS"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutesRecords, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutes2Records, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hoursRecords, expectedHours), "hours - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hours2Records, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(daysRecords, expectedDays), "days - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(days2Records, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("SECONDS");
            assertTimestamps("MINUTES");
            assertTimestamps("HOURS");
            assertTimestamps("DAYS");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

//    @Test // TODO Passes
    public void testContinuousSecToMonth() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousSecToMonth");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertMinutesStream(startTime string, endTime string);\n" +
                        "define stream AssertHoursStream(startTime string, endTime string);\n" +
                        "define stream AssertDaysStream(startTime string, endTime string);\n" +
                        "define stream AssertMonthsStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    count() as totalRequestCount,\n" +
                        "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                        "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every sec...month;\n" +


                        /* Per minutes */
                        // Group by: context
                        "\n@info(name = 'assertMinutes')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMinutes2')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per hours */
                        // Group by: context
                        "\n@info(name = 'assertHours')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertHours2')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per days */
                        // Group by: context
                        "\n@info(name = 'assertDays')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertDays2')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per months */
                        // Group by: context
                        "\n@info(name = 'assertMonths')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMonths2')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert minutes
            AssertionInfoHolder assertMinutesInfo = new AssertionInfoHolder();
            addCallback("assertMinutes", siddhiAppRuntime, assertMinutesInfo);
            AssertionInfoHolder assertMinutes2Info = new AssertionInfoHolder();
            addCallback("assertMinutes2", siddhiAppRuntime, assertMinutes2Info);

            // Info to assert hours
            AssertionInfoHolder assertHoursInfo = new AssertionInfoHolder();
            addCallback("assertHours", siddhiAppRuntime, assertHoursInfo);
            AssertionInfoHolder assertHours2Info = new AssertionInfoHolder();
            addCallback("assertHours2", siddhiAppRuntime, assertHours2Info);

            // Info to assert days
            AssertionInfoHolder assertDaysInfo = new AssertionInfoHolder();
            addCallback("assertDays", siddhiAppRuntime, assertDaysInfo);
            AssertionInfoHolder assertDays2Info = new AssertionInfoHolder();
            addCallback("assertDays2", siddhiAppRuntime, assertDays2Info);

            // Info to assert months
            AssertionInfoHolder assertMonthsInfo = new AssertionInfoHolder();
            addCallback("assertMonths", siddhiAppRuntime, assertMonthsInfo);
            AssertionInfoHolder assertMonths2Info = new AssertionInfoHolder();
            addCallback("assertMonths2", siddhiAppRuntime, assertMonths2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertMinutesStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMinutesStream");
            InputHandler assertHoursStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertHoursStream");
            InputHandler assertDaysStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertDaysStream");
            InputHandler assertMonthsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMonthsStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.MINUTES);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertMinutesStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertHoursStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertDaysStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertMonthsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events
            List<Object[]> expectedMinutes = Arrays.asList(
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591074060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984060000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedMinutes2 = Arrays.asList(
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530417660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527915660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590984060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0});

            List<Object[]> expectedHours = Arrays.asList(
                    new Object[]{1591074000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593666000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590984000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527915600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591070400000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530417600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedHours2 = Arrays.asList(
                    new Object[]{1591074000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593576000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530504000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527915600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527829200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593666000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1590987600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527912000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 3.0,3.0,3.0,3.0});

            List<Object[]> expectedDays = Arrays.asList(
                    new Object[]{1590969600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1530489600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593648000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1590969600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593561600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530489600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593648000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 1.5,6.0,1.0,2.0});

            List<Object[]> expectedDays2 = Arrays.asList(
                    new Object[]{1590969600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530489600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527811200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593648000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593561600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1527897600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593561600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527811200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530489600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593561600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1530489600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1591056000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1593561600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527897600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1590969600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527897600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593648000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1530489600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527897600000L, 2L, 4.0,8.0,4.0,4.0});

            List<Object[]> expectedMonths = Arrays.asList(
                    new Object[]{1530403200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1590969600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1527811200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1593561600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1530403200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1527811200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1593561600000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1590969600000L, 8L, 1.5,12.0,1.0,2.0});

            List<Object[]> expectedMonths2 = Arrays.asList(
                    new Object[]{1530403200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1593561600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1530403200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1527811200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1593561600000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1590969600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1593561600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1527811200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1527811200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1590969600000L, 4L, 2.0,8.0,2.0,2.0});


            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 32, assertMinutesInfo.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertMinutes2Info.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 32, assertHoursInfo.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertHours2Info.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 16, assertDaysInfo.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 32, assertDays2Info.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 8, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 16, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutesInfo.events, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutes2Info.events, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHoursInfo.events, expectedHours), "hours - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHours2Info.events, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDaysInfo.events, expectedDays), "days - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDays2Info.events, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonthsInfo.events, expectedMonths), "months - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonths2Info.events, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> minutesRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MINUTES"));
            List<Object[]> minutes2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MINUTES"));
            List<Object[]> hoursRecords = runStatementAndGetResult(generateAggregatedAssertQuery("HOURS"));
            List<Object[]> hours2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("HOURS"));
            List<Object[]> daysRecords = runStatementAndGetResult(generateAggregatedAssertQuery("DAYS"));
            List<Object[]> days2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("DAYS"));
            List<Object[]> monthsRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MONTHS"));
            List<Object[]> months2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MONTHS"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutesRecords, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutes2Records, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hoursRecords, expectedHours), "hours - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hours2Records, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(daysRecords, expectedDays), "days - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(days2Records, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(monthsRecords, expectedMonths), "months - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(months2Records, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("SECONDS");
            assertTimestamps("MINUTES");
            assertTimestamps("HOURS");
            assertTimestamps("DAYS");
            assertTimestamps("MONTHS");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test // TODO passed
    public void testContinuousSecToYear() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousSecToYear");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
//        siddhiManager.setDataSource("TEST_DATASOURCE", RDBMSTableTestUtils.getTestDataSource()); // TODO no need
        String siddhiAppBody =
            "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                "define stream AssertMinutesStream(startTime string, endTime string);\n" +
                "define stream AssertHoursStream(startTime string, endTime string);\n" +
                "define stream AssertDaysStream(startTime string, endTime string);\n" +
                "define stream AssertMonthsStream(startTime string, endTime string);\n" +
                "define stream AssertYearsStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                getStoreAnnotation() +
                "define aggregation REQUEST_SUMMARY \n" +
                "from RequestStream\n" +
                "select \n" +
                "    requestTimestamp, context, version, userId, \n" +
                "    count() as totalRequestCount,\n" +
                "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                "group by context, version, userId\n" +
                "aggregate by requestTimestamp every sec...year;\n" +


                /* Per minutes */
                // Group by: context
                "\n@info(name = 'assertMinutes')\n" +
                "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                "    within startTime, endTime\n" +
                "    per \"minutes\"\n" +
                "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                "group by AGG_TIMESTAMP, context\n" +
                "insert into AssertOutputDiscardStream;\n" +

                // Group by: context, version
                "\n@info(name = 'assertMinutes2')\n" +
                "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                "    within startTime, endTime\n" +
                "    per \"minutes\"\n" +
                "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                "group by AGG_TIMESTAMP, context, version\n" +
                "insert into AssertOutputDiscardStream;\n" +


                /* Per hours */
                // Group by: context
                "\n@info(name = 'assertHours')\n" +
                "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                "    within startTime, endTime\n" +
                "    per \"hours\"\n" +
                "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                "group by AGG_TIMESTAMP, context\n" +
                "insert into AssertOutputDiscardStream;\n" +

                // Group by: context, version
                "\n@info(name = 'assertHours2')\n" +
                "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                "    within startTime, endTime\n" +
                "    per \"hours\"\n" +
                "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                "group by AGG_TIMESTAMP, context, version\n" +
                "insert into AssertOutputDiscardStream;\n" +


                /* Per days */
                // Group by: context
                "\n@info(name = 'assertDays')\n" +
                "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                "    within startTime, endTime\n" +
                "    per \"days\"\n" +
                "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                "group by AGG_TIMESTAMP, context\n" +
                "insert into AssertOutputDiscardStream;\n" +

                // Group by: context, version
                "\n@info(name = 'assertDays2')\n" +
                "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                "    within startTime, endTime\n" +
                "    per \"days\"\n" +
                "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                "group by AGG_TIMESTAMP, context, version\n" +
                "insert into AssertOutputDiscardStream;\n" +


                /* Per months */
                // Group by: context
                "\n@info(name = 'assertMonths')\n" +
                "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                "    within startTime, endTime\n" +
                "    per \"months\"\n" +
                "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                "group by AGG_TIMESTAMP, context\n" +
                "insert into AssertOutputDiscardStream;\n" +

                // Group by: context, version
                "\n@info(name = 'assertMonths2')\n" +
                "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                "    within startTime, endTime\n" +
                "    per \"months\"\n" +
                "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                "group by AGG_TIMESTAMP, context, version\n" +
                "insert into AssertOutputDiscardStream;\n" +


                /* Per years */
                // Group by: context
                "\n@info(name = 'assertYears')\n" +
                "from AssertYearsStream as S join REQUEST_SUMMARY as A\n" +
                "    within startTime, endTime\n" +
                "    per \"years\"\n" +
                "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                "group by AGG_TIMESTAMP, context\n" +
                "insert into AssertOutputDiscardStream;\n" +

                // Group by: context, version
                "\n@info(name = 'assertYears2')\n" +
                "from AssertYearsStream as S join REQUEST_SUMMARY as A\n" +
                "    within startTime, endTime\n" +
                "    per \"years\"\n" +
                "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                "group by AGG_TIMESTAMP, context, version\n" +
                "insert into AssertOutputDiscardStream;\n" +

                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert minutes
            AssertionInfoHolder assertMinutesInfo = new AssertionInfoHolder();
            addCallback("assertMinutes", siddhiAppRuntime, assertMinutesInfo);
            AssertionInfoHolder assertMinutes2Info = new AssertionInfoHolder();
            addCallback("assertMinutes2", siddhiAppRuntime, assertMinutes2Info);

            // Info to assert hours
            AssertionInfoHolder assertHoursInfo = new AssertionInfoHolder();
            addCallback("assertHours", siddhiAppRuntime, assertHoursInfo);
            AssertionInfoHolder assertHours2Info = new AssertionInfoHolder();
            addCallback("assertHours2", siddhiAppRuntime, assertHours2Info);

            // Info to assert days
            AssertionInfoHolder assertDaysInfo = new AssertionInfoHolder();
            addCallback("assertDays", siddhiAppRuntime, assertDaysInfo);
            AssertionInfoHolder assertDays2Info = new AssertionInfoHolder();
            addCallback("assertDays2", siddhiAppRuntime, assertDays2Info);

            // Info to assert months
            AssertionInfoHolder assertMonthsInfo = new AssertionInfoHolder();
            addCallback("assertMonths", siddhiAppRuntime, assertMonthsInfo);
            AssertionInfoHolder assertMonths2Info = new AssertionInfoHolder();
            addCallback("assertMonths2", siddhiAppRuntime, assertMonths2Info);

            // Info to assert years
            AssertionInfoHolder assertYearsInfo = new AssertionInfoHolder();
            addCallback("assertYears", siddhiAppRuntime, assertYearsInfo);
            AssertionInfoHolder assertYears2Info = new AssertionInfoHolder();
            addCallback("assertYears2", siddhiAppRuntime, assertYears2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertMinutesStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMinutesStream");
            InputHandler assertHoursStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertHoursStream");
            InputHandler assertDaysStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertDaysStream");
            InputHandler assertMonthsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMonthsStream");
            InputHandler assertYearsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertYearsStream");

            siddhiAppRuntime.start();

            log.info("Waiting till siddhi app starts for 10000s");
            Thread.sleep(10000);

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.MINUTES);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertMinutesStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertHoursStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertDaysStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertMonthsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertYearsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events
            List<Object[]> expectedMinutes = Arrays.asList(
                new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1527825660000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1530417660000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1530421260000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1593576060000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1590987660000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1530504060000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1527912060000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1593662460000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1527915660000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1527829260000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1593579660000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1591070460000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1591074060000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1530507660000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1593666060000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1590984060000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedMinutes2 = Arrays.asList(
                new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1527825660000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1530417660000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1530421260000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1593576060000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1590987660000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1530504060000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1527912060000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1593662460000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1527915660000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1527829260000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1593579660000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1593579660000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1527829260000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1530421260000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1591070460000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1593662460000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1530504060000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1591074060000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1527912060000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1590987660000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1530507660000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1527825660000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1527915660000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1593666060000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1530507660000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1530417660000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1593576060000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1591070460000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1590984060000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1593666060000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1591074060000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1590984060000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0});

            List<Object[]> expectedHours = Arrays.asList(
                new Object[]{1591074000000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1593576000000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1593666000000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1527829200000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1593579600000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1527825600000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1530504000000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1590984000000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1530421200000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1527915600000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1530507600000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1591070400000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1530417600000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                new Object[]{1590987600000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1527912000000L, 2L, 3.5,7.0,3.0,4.0},
                new Object[]{1593662400000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedHours2 = Arrays.asList(
                new Object[]{1591074000000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1593576000000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1593666000000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1527829200000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1593579600000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1527825600000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1530504000000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1590984000000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1591074000000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1530421200000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1530421200000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1527825600000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1527915600000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1530504000000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1527829200000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1593579600000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1530507600000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1527915600000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1593666000000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1591070400000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1590984000000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1530417600000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1590987600000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1527912000000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1590987600000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1593576000000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1593662400000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1530417600000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1591070400000L, 1L, 3.0,3.0,3.0,3.0},
                new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0},
                new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                new Object[]{1527912000000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1593662400000L, 1L, 4.0,4.0,4.0,4.0},
                new Object[]{1530507600000L, 1L, 3.0,3.0,3.0,3.0});

            List<Object[]> expectedDays = Arrays.asList(
                new Object[]{1590969600000L, 4L, 3.5,14.0,3.0,4.0},
                new Object[]{1530403200000L, 4L, 3.5,14.0,3.0,4.0},
                new Object[]{1530403200000L, 4L, 1.5,6.0,1.0,2.0},
                new Object[]{1530489600000L, 4L, 3.5,14.0,3.0,4.0},
                new Object[]{1527811200000L, 4L, 1.5,6.0,1.0,2.0},
                new Object[]{1593648000000L, 4L, 3.5,14.0,3.0,4.0},
                new Object[]{1593561600000L, 4L, 1.5,6.0,1.0,2.0},
                new Object[]{1591056000000L, 4L, 1.5,6.0,1.0,2.0},
                new Object[]{1527897600000L, 4L, 3.5,14.0,3.0,4.0},
                new Object[]{1590969600000L, 4L, 1.5,6.0,1.0,2.0},
                new Object[]{1593561600000L, 4L, 3.5,14.0,3.0,4.0},
                new Object[]{1527811200000L, 4L, 3.5,14.0,3.0,4.0},
                new Object[]{1530489600000L, 4L, 1.5,6.0,1.0,2.0},
                new Object[]{1591056000000L, 4L, 3.5,14.0,3.0,4.0},
                new Object[]{1593648000000L, 4L, 1.5,6.0,1.0,2.0},
                new Object[]{1527897600000L, 4L, 1.5,6.0,1.0,2.0});

            List<Object[]> expectedDays2 = Arrays.asList(
                new Object[]{1590969600000L, 2L, 4.0,8.0,4.0,4.0},
                new Object[]{1530403200000L, 2L, 4.0,8.0,4.0,4.0},
                new Object[]{1530403200000L, 2L, 1.0,2.0,1.0,1.0},
                new Object[]{1530489600000L, 2L, 3.0,6.0,3.0,3.0},
                new Object[]{1527811200000L, 2L, 2.0,4.0,2.0,2.0},
                new Object[]{1593648000000L, 2L, 3.0,6.0,3.0,3.0},
                new Object[]{1593561600000L, 2L, 2.0,4.0,2.0,2.0},
                new Object[]{1591056000000L, 2L, 2.0,4.0,2.0,2.0},
                new Object[]{1527897600000L, 2L, 3.0,6.0,3.0,3.0},
                new Object[]{1590969600000L, 2L, 1.0,2.0,1.0,1.0},
                new Object[]{1593561600000L, 2L, 4.0,8.0,4.0,4.0},
                new Object[]{1527811200000L, 2L, 4.0,8.0,4.0,4.0},
                new Object[]{1530489600000L, 2L, 2.0,4.0,2.0,2.0},
                new Object[]{1593561600000L, 2L, 1.0,2.0,1.0,1.0},
                new Object[]{1527811200000L, 2L, 1.0,2.0,1.0,1.0},
                new Object[]{1530403200000L, 2L, 2.0,4.0,2.0,2.0},
                new Object[]{1591056000000L, 2L, 3.0,6.0,3.0,3.0},
                new Object[]{1590969600000L, 2L, 2.0,4.0,2.0,2.0},
                new Object[]{1530489600000L, 2L, 1.0,2.0,1.0,1.0},
                new Object[]{1527811200000L, 2L, 3.0,6.0,3.0,3.0},
                new Object[]{1593648000000L, 2L, 4.0,8.0,4.0,4.0},
                new Object[]{1591056000000L, 2L, 4.0,8.0,4.0,4.0},
                new Object[]{1593561600000L, 2L, 3.0,6.0,3.0,3.0},
                new Object[]{1593648000000L, 2L, 1.0,2.0,1.0,1.0},
                new Object[]{1527897600000L, 2L, 2.0,4.0,2.0,2.0},
                new Object[]{1590969600000L, 2L, 3.0,6.0,3.0,3.0},
                new Object[]{1527897600000L, 2L, 1.0,2.0,1.0,1.0},
                new Object[]{1593648000000L, 2L, 2.0,4.0,2.0,2.0},
                new Object[]{1591056000000L, 2L, 1.0,2.0,1.0,1.0},
                new Object[]{1530403200000L, 2L, 3.0,6.0,3.0,3.0},
                new Object[]{1530489600000L, 2L, 4.0,8.0,4.0,4.0},
                new Object[]{1527897600000L, 2L, 4.0,8.0,4.0,4.0});

            List<Object[]> expectedMonths = Arrays.asList(
                new Object[]{1530403200000L, 8L, 3.5,28.0,3.0,4.0},
                new Object[]{1590969600000L, 8L, 3.5,28.0,3.0,4.0},
                new Object[]{1527811200000L, 8L, 3.5,28.0,3.0,4.0},
                new Object[]{1593561600000L, 8L, 3.5,28.0,3.0,4.0},
                new Object[]{1530403200000L, 8L, 1.5,12.0,1.0,2.0},
                new Object[]{1527811200000L, 8L, 1.5,12.0,1.0,2.0},
                new Object[]{1593561600000L, 8L, 1.5,12.0,1.0,2.0},
                new Object[]{1590969600000L, 8L, 1.5,12.0,1.0,2.0});

            List<Object[]> expectedMonths2 = Arrays.asList(
                new Object[]{1530403200000L, 4L, 4.0,16.0,4.0,4.0},
                new Object[]{1590969600000L, 4L, 4.0,16.0,4.0,4.0},
                new Object[]{1527811200000L, 4L, 3.0,12.0,3.0,3.0},
                new Object[]{1593561600000L, 4L, 3.0,12.0,3.0,3.0},
                new Object[]{1530403200000L, 4L, 1.0,4.0,1.0,1.0},
                new Object[]{1527811200000L, 4L, 2.0,8.0,2.0,2.0},
                new Object[]{1593561600000L, 4L, 2.0,8.0,2.0,2.0},
                new Object[]{1590969600000L, 4L, 1.0,4.0,1.0,1.0},
                new Object[]{1593561600000L, 4L, 4.0,16.0,4.0,4.0},
                new Object[]{1590969600000L, 4L, 3.0,12.0,3.0,3.0},
                new Object[]{1527811200000L, 4L, 4.0,16.0,4.0,4.0},
                new Object[]{1593561600000L, 4L, 1.0,4.0,1.0,1.0},
                new Object[]{1530403200000L, 4L, 2.0,8.0,2.0,2.0},
                new Object[]{1527811200000L, 4L, 1.0,4.0,1.0,1.0},
                new Object[]{1530403200000L, 4L, 3.0,12.0,3.0,3.0},
                new Object[]{1590969600000L, 4L, 2.0,8.0,2.0,2.0});

            List<Object[]> expectedYears = Arrays.asList(
                new Object[]{1514764800000L, 16L, 1.5,24.0,1.0,2.0},
                new Object[]{1577836800000L, 16L, 1.5,24.0,1.0,2.0},
                new Object[]{1514764800000L, 16L, 3.5,56.0,3.0,4.0},
                new Object[]{1577836800000L, 16L, 3.5,56.0,3.0,4.0});

            List<Object[]> expectedYears2 = Arrays.asList(
                new Object[]{1514764800000L, 8L, 2.0,16.0,2.0,2.0},
                new Object[]{1577836800000L, 8L, 1.0,8.0,1.0,1.0},
                new Object[]{1514764800000L, 8L, 4.0,32.0,4.0,4.0},
                new Object[]{1577836800000L, 8L, 2.0,16.0,2.0,2.0},
                new Object[]{1514764800000L, 8L, 3.0,24.0,3.0,3.0},
                new Object[]{1577836800000L, 8L, 3.0,24.0,3.0,3.0},
                new Object[]{1514764800000L, 8L, 1.0,8.0,1.0,1.0},
                new Object[]{1577836800000L, 8L, 4.0,32.0,4.0,4.0});


            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 32, assertMinutesInfo.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertMinutes2Info.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 32, assertHoursInfo.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertHours2Info.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 16, assertDaysInfo.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 32, assertDays2Info.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 8, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 16, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 4, assertYearsInfo.eventCount, 60000);
            log.info("Wait over for: years - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 8, assertYearsInfo.eventCount, 60000);
            log.info("Wait over for: years - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutesInfo.events, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutes2Info.events, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHoursInfo.events, expectedHours), "hours - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHours2Info.events, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDaysInfo.events, expectedDays), "days - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDays2Info.events, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonthsInfo.events, expectedMonths), "months - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonths2Info.events, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertYearsInfo.events, expectedYears), "years - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertYears2Info.events, expectedYears2), "years - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> minutesRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MINUTES"));
            List<Object[]> minutes2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MINUTES"));
            List<Object[]> hoursRecords = runStatementAndGetResult(generateAggregatedAssertQuery("HOURS"));
            List<Object[]> hours2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("HOURS"));
            List<Object[]> daysRecords = runStatementAndGetResult(generateAggregatedAssertQuery("DAYS"));
            List<Object[]> days2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("DAYS"));
            List<Object[]> monthsRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MONTHS"));
            List<Object[]> months2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MONTHS"));
            List<Object[]> yearsRecords = runStatementAndGetResult(generateAggregatedAssertQuery("YEARS"));
            List<Object[]> years2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("YEARS"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutesRecords, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutes2Records, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hoursRecords, expectedHours), "hours - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hours2Records, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(daysRecords, expectedDays), "days - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(days2Records, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(monthsRecords, expectedMonths), "months - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(months2Records, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(yearsRecords, expectedYears), "years - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(years2Records, expectedYears2), "years - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("SECONDS");
            assertTimestamps("MINUTES");
            assertTimestamps("HOURS");
            assertTimestamps("DAYS");
            assertTimestamps("MONTHS");
            assertTimestamps("YEARS");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    // Starting from sec [END]

    // Starting from min [BEGIN]

//    @Test // TODO Passes
    public void testContinuousMinToHour() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousMinToHour");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertMinutesStream(startTime string, endTime string);\n" +
                        "define stream AssertHoursStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    count() as totalRequestCount,\n" +
                        "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                        "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every min...hour;\n" +


                        /* Per minutes */
                        // Group by: context
                        "\n@info(name = 'assertMinutes')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMinutes2')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per hours */
                        // Group by: context
                        "\n@info(name = 'assertHours')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertHours2')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert minutes
            AssertionInfoHolder assertMinutesInfo = new AssertionInfoHolder();
            addCallback("assertMinutes", siddhiAppRuntime, assertMinutesInfo);
            AssertionInfoHolder assertMinutes2Info = new AssertionInfoHolder();
            addCallback("assertMinutes2", siddhiAppRuntime, assertMinutes2Info);

            // Info to assert hours
            AssertionInfoHolder assertHoursInfo = new AssertionInfoHolder();
            addCallback("assertHours", siddhiAppRuntime, assertHoursInfo);
            AssertionInfoHolder assertHours2Info = new AssertionInfoHolder();
            addCallback("assertHours2", siddhiAppRuntime, assertHours2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertMinutesStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMinutesStream");
            InputHandler assertHoursStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertHoursStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.MINUTES);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertMinutesStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertHoursStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events
            List<Object[]> expectedMinutes = Arrays.asList(
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591074060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984060000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedMinutes2 = Arrays.asList(
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530417660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527915660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590984060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0});

            List<Object[]> expectedHours = Arrays.asList(
                    new Object[]{1591074000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593666000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590984000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527915600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591070400000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530417600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedHours2 = Arrays.asList(
                    new Object[]{1591074000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593576000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530504000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527915600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527829200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593666000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1590987600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527912000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 3.0,3.0,3.0,3.0});

            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 32, assertMinutesInfo.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertMinutes2Info.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 32, assertHoursInfo.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertHours2Info.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutesInfo.events, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutes2Info.events, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHoursInfo.events, expectedHours), "hours - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHours2Info.events, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> minutesRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MINUTES"));
            List<Object[]> minutes2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MINUTES"));
            List<Object[]> hoursRecords = runStatementAndGetResult(generateAggregatedAssertQuery("HOURS"));
            List<Object[]> hours2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("HOURS"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutesRecords, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutes2Records, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hoursRecords, expectedHours), "hours - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hours2Records, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("MINUTES");
            assertTimestamps("HOURS");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

//    @Test // TODO Passes
    public void testContinuousMinToDay() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousMinToDay");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertMinutesStream(startTime string, endTime string);\n" +
                        "define stream AssertHoursStream(startTime string, endTime string);\n" +
                        "define stream AssertDaysStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    count() as totalRequestCount,\n" +
                        "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                        "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every min...day;\n" +


                        /* Per minutes */
                        // Group by: context
                        "\n@info(name = 'assertMinutes')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMinutes2')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per hours */
                        // Group by: context
                        "\n@info(name = 'assertHours')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertHours2')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per days */
                        // Group by: context
                        "\n@info(name = 'assertDays')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertDays2')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert minutes
            AssertionInfoHolder assertMinutesInfo = new AssertionInfoHolder();
            addCallback("assertMinutes", siddhiAppRuntime, assertMinutesInfo);
            AssertionInfoHolder assertMinutes2Info = new AssertionInfoHolder();
            addCallback("assertMinutes2", siddhiAppRuntime, assertMinutes2Info);

            // Info to assert hours
            AssertionInfoHolder assertHoursInfo = new AssertionInfoHolder();
            addCallback("assertHours", siddhiAppRuntime, assertHoursInfo);
            AssertionInfoHolder assertHours2Info = new AssertionInfoHolder();
            addCallback("assertHours2", siddhiAppRuntime, assertHours2Info);

            // Info to assert days
            AssertionInfoHolder assertDaysInfo = new AssertionInfoHolder();
            addCallback("assertDays", siddhiAppRuntime, assertDaysInfo);
            AssertionInfoHolder assertDays2Info = new AssertionInfoHolder();
            addCallback("assertDays2", siddhiAppRuntime, assertDays2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertMinutesStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMinutesStream");
            InputHandler assertHoursStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertHoursStream");
            InputHandler assertDaysStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertDaysStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.MINUTES);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertMinutesStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertHoursStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertDaysStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events
            List<Object[]> expectedMinutes = Arrays.asList(
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591074060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984060000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedMinutes2 = Arrays.asList(
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530417660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527915660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590984060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0});

            List<Object[]> expectedHours = Arrays.asList(
                    new Object[]{1591074000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593666000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590984000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527915600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591070400000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530417600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedHours2 = Arrays.asList(
                    new Object[]{1591074000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593576000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530504000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527915600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527829200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593666000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1590987600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527912000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 3.0,3.0,3.0,3.0});

            List<Object[]> expectedDays = Arrays.asList(
                    new Object[]{1590969600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1530489600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593648000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1590969600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593561600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530489600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593648000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 1.5,6.0,1.0,2.0});

            List<Object[]> expectedDays2 = Arrays.asList(
                    new Object[]{1590969600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530489600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527811200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593648000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593561600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1527897600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593561600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527811200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530489600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593561600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1530489600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1591056000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1593561600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527897600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1590969600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527897600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593648000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1530489600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527897600000L, 2L, 4.0,8.0,4.0,4.0});


            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 32, assertMinutesInfo.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertMinutes2Info.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 32, assertHoursInfo.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertHours2Info.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 16, assertDaysInfo.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 32, assertDays2Info.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutesInfo.events, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutes2Info.events, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHoursInfo.events, expectedHours), "hours - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHours2Info.events, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDaysInfo.events, expectedDays), "days - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDays2Info.events, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> minutesRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MINUTES"));
            List<Object[]> minutes2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MINUTES"));
            List<Object[]> hoursRecords = runStatementAndGetResult(generateAggregatedAssertQuery("HOURS"));
            List<Object[]> hours2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("HOURS"));
            List<Object[]> daysRecords = runStatementAndGetResult(generateAggregatedAssertQuery("DAYS"));
            List<Object[]> days2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("DAYS"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutesRecords, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutes2Records, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hoursRecords, expectedHours), "hours - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hours2Records, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(daysRecords, expectedDays), "days - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(days2Records, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("MINUTES");
            assertTimestamps("HOURS");
            assertTimestamps("DAYS");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

//    @Test // TODO Passed
    public void testContinuousMinToMonth() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousMinToMonth");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertMinutesStream(startTime string, endTime string);\n" +
                        "define stream AssertHoursStream(startTime string, endTime string);\n" +
                        "define stream AssertDaysStream(startTime string, endTime string);\n" +
                        "define stream AssertMonthsStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    count() as totalRequestCount,\n" +
                        "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                        "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every min...month;\n" +


                        /* Per minutes */
                        // Group by: context
                        "\n@info(name = 'assertMinutes')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMinutes2')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per hours */
                        // Group by: context
                        "\n@info(name = 'assertHours')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertHours2')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per days */
                        // Group by: context
                        "\n@info(name = 'assertDays')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertDays2')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per months */
                        // Group by: context
                        "\n@info(name = 'assertMonths')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMonths2')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert minutes
            AssertionInfoHolder assertMinutesInfo = new AssertionInfoHolder();
            addCallback("assertMinutes", siddhiAppRuntime, assertMinutesInfo);
            AssertionInfoHolder assertMinutes2Info = new AssertionInfoHolder();
            addCallback("assertMinutes2", siddhiAppRuntime, assertMinutes2Info);

            // Info to assert hours
            AssertionInfoHolder assertHoursInfo = new AssertionInfoHolder();
            addCallback("assertHours", siddhiAppRuntime, assertHoursInfo);
            AssertionInfoHolder assertHours2Info = new AssertionInfoHolder();
            addCallback("assertHours2", siddhiAppRuntime, assertHours2Info);

            // Info to assert days
            AssertionInfoHolder assertDaysInfo = new AssertionInfoHolder();
            addCallback("assertDays", siddhiAppRuntime, assertDaysInfo);
            AssertionInfoHolder assertDays2Info = new AssertionInfoHolder();
            addCallback("assertDays2", siddhiAppRuntime, assertDays2Info);

            // Info to assert months
            AssertionInfoHolder assertMonthsInfo = new AssertionInfoHolder();
            addCallback("assertMonths", siddhiAppRuntime, assertMonthsInfo);
            AssertionInfoHolder assertMonths2Info = new AssertionInfoHolder();
            addCallback("assertMonths2", siddhiAppRuntime, assertMonths2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertMinutesStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMinutesStream");
            InputHandler assertHoursStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertHoursStream");
            InputHandler assertDaysStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertDaysStream");
            InputHandler assertMonthsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMonthsStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.MINUTES);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertMinutesStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertHoursStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertDaysStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertMonthsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events
            List<Object[]> expectedMinutes = Arrays.asList(
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591074060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984060000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedMinutes2 = Arrays.asList(
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530417660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527915660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590984060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0});

            List<Object[]> expectedHours = Arrays.asList(
                    new Object[]{1591074000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593666000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590984000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527915600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591070400000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530417600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedHours2 = Arrays.asList(
                    new Object[]{1591074000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593576000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530504000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527915600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527829200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593666000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1590987600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527912000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 3.0,3.0,3.0,3.0});

            List<Object[]> expectedDays = Arrays.asList(
                    new Object[]{1590969600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1530489600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593648000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1590969600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593561600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530489600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593648000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 1.5,6.0,1.0,2.0});

            List<Object[]> expectedDays2 = Arrays.asList(
                    new Object[]{1590969600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530489600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527811200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593648000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593561600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1527897600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593561600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527811200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530489600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593561600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1530489600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1591056000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1593561600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527897600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1590969600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527897600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593648000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1530489600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527897600000L, 2L, 4.0,8.0,4.0,4.0});

            List<Object[]> expectedMonths = Arrays.asList(
                    new Object[]{1530403200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1590969600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1527811200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1593561600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1530403200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1527811200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1593561600000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1590969600000L, 8L, 1.5,12.0,1.0,2.0});

            List<Object[]> expectedMonths2 = Arrays.asList(
                    new Object[]{1530403200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1593561600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1530403200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1527811200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1593561600000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1590969600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1593561600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1527811200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1527811200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1590969600000L, 4L, 2.0,8.0,2.0,2.0});


            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 32, assertMinutesInfo.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertMinutes2Info.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 32, assertHoursInfo.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertHours2Info.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 16, assertDaysInfo.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 32, assertDays2Info.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 8, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 16, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutesInfo.events, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutes2Info.events, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHoursInfo.events, expectedHours), "hours - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHours2Info.events, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDaysInfo.events, expectedDays), "days - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDays2Info.events, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonthsInfo.events, expectedMonths), "months - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonths2Info.events, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> minutesRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MINUTES"));
            List<Object[]> minutes2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MINUTES"));
            List<Object[]> hoursRecords = runStatementAndGetResult(generateAggregatedAssertQuery("HOURS"));
            List<Object[]> hours2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("HOURS"));
            List<Object[]> daysRecords = runStatementAndGetResult(generateAggregatedAssertQuery("DAYS"));
            List<Object[]> days2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("DAYS"));
            List<Object[]> monthsRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MONTHS"));
            List<Object[]> months2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MONTHS"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutesRecords, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutes2Records, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hoursRecords, expectedHours), "hours - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hours2Records, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(daysRecords, expectedDays), "days - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(days2Records, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(monthsRecords, expectedMonths), "months - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(months2Records, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("MINUTES");
            assertTimestamps("HOURS");
            assertTimestamps("DAYS");
            assertTimestamps("MONTHS");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

//    @Test // TODO Passes
    public void testContinuousMinToYear() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousMinToYear");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertMinutesStream(startTime string, endTime string);\n" +
                        "define stream AssertHoursStream(startTime string, endTime string);\n" +
                        "define stream AssertDaysStream(startTime string, endTime string);\n" +
                        "define stream AssertMonthsStream(startTime string, endTime string);\n" +
                        "define stream AssertYearsStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    count() as totalRequestCount,\n" +
                        "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                        "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every min...year;\n" +


                        /* Per minutes */
                        // Group by: context
                        "\n@info(name = 'assertMinutes')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMinutes2')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per hours */
                        // Group by: context
                        "\n@info(name = 'assertHours')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertHours2')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per days */
                        // Group by: context
                        "\n@info(name = 'assertDays')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertDays2')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per months */
                        // Group by: context
                        "\n@info(name = 'assertMonths')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMonths2')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per years */
                        // Group by: context
                        "\n@info(name = 'assertYears')\n" +
                        "from AssertYearsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"years\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertYears2')\n" +
                        "from AssertYearsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"years\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert minutes
            AssertionInfoHolder assertMinutesInfo = new AssertionInfoHolder();
            addCallback("assertMinutes", siddhiAppRuntime, assertMinutesInfo);
            AssertionInfoHolder assertMinutes2Info = new AssertionInfoHolder();
            addCallback("assertMinutes2", siddhiAppRuntime, assertMinutes2Info);

            // Info to assert hours
            AssertionInfoHolder assertHoursInfo = new AssertionInfoHolder();
            addCallback("assertHours", siddhiAppRuntime, assertHoursInfo);
            AssertionInfoHolder assertHours2Info = new AssertionInfoHolder();
            addCallback("assertHours2", siddhiAppRuntime, assertHours2Info);

            // Info to assert days
            AssertionInfoHolder assertDaysInfo = new AssertionInfoHolder();
            addCallback("assertDays", siddhiAppRuntime, assertDaysInfo);
            AssertionInfoHolder assertDays2Info = new AssertionInfoHolder();
            addCallback("assertDays2", siddhiAppRuntime, assertDays2Info);

            // Info to assert months
            AssertionInfoHolder assertMonthsInfo = new AssertionInfoHolder();
            addCallback("assertMonths", siddhiAppRuntime, assertMonthsInfo);
            AssertionInfoHolder assertMonths2Info = new AssertionInfoHolder();
            addCallback("assertMonths2", siddhiAppRuntime, assertMonths2Info);

            // Info to assert years
            AssertionInfoHolder assertYearsInfo = new AssertionInfoHolder();
            addCallback("assertYears", siddhiAppRuntime, assertYearsInfo);
            AssertionInfoHolder assertYears2Info = new AssertionInfoHolder();
            addCallback("assertYears2", siddhiAppRuntime, assertYears2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertMinutesStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMinutesStream");
            InputHandler assertHoursStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertHoursStream");
            InputHandler assertDaysStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertDaysStream");
            InputHandler assertMonthsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMonthsStream");
            InputHandler assertYearsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertYearsStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.MINUTES);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertMinutesStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertHoursStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertDaysStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertMonthsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertYearsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events
            List<Object[]> expectedMinutes = Arrays.asList(
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590987660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829260000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070460000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591074060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507660000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666060000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984060000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedMinutes2 = Arrays.asList(
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530417660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527829260000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421260000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593662460000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527912060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590987660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1527915660000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417660000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591070460000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590984060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666060000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984060000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0});

            List<Object[]> expectedHours = Arrays.asList(
                    new Object[]{1591074000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593666000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590984000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527915600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591070400000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530417600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedHours2 = Arrays.asList(
                    new Object[]{1591074000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593576000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530504000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527915600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527829200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593666000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1590987600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527912000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 3.0,3.0,3.0,3.0});

            List<Object[]> expectedDays = Arrays.asList(
                    new Object[]{1590969600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1530489600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593648000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1590969600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593561600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530489600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593648000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 1.5,6.0,1.0,2.0});

            List<Object[]> expectedDays2 = Arrays.asList(
                    new Object[]{1590969600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530489600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527811200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593648000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593561600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1527897600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593561600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527811200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530489600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593561600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1530489600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1591056000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1593561600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527897600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1590969600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527897600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593648000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1530489600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527897600000L, 2L, 4.0,8.0,4.0,4.0});

            List<Object[]> expectedMonths = Arrays.asList(
                    new Object[]{1530403200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1590969600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1527811200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1593561600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1530403200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1527811200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1593561600000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1590969600000L, 8L, 1.5,12.0,1.0,2.0});

            List<Object[]> expectedMonths2 = Arrays.asList(
                    new Object[]{1530403200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1593561600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1530403200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1527811200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1593561600000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1590969600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1593561600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1527811200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1527811200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1590969600000L, 4L, 2.0,8.0,2.0,2.0});

            List<Object[]> expectedYears = Arrays.asList(
                    new Object[]{1514764800000L, 16L, 1.5,24.0,1.0,2.0},
                    new Object[]{1577836800000L, 16L, 1.5,24.0,1.0,2.0},
                    new Object[]{1514764800000L, 16L, 3.5,56.0,3.0,4.0},
                    new Object[]{1577836800000L, 16L, 3.5,56.0,3.0,4.0});

            List<Object[]> expectedYears2 = Arrays.asList(
                    new Object[]{1514764800000L, 8L, 2.0,16.0,2.0,2.0},
                    new Object[]{1577836800000L, 8L, 1.0,8.0,1.0,1.0},
                    new Object[]{1514764800000L, 8L, 4.0,32.0,4.0,4.0},
                    new Object[]{1577836800000L, 8L, 2.0,16.0,2.0,2.0},
                    new Object[]{1514764800000L, 8L, 3.0,24.0,3.0,3.0},
                    new Object[]{1577836800000L, 8L, 3.0,24.0,3.0,3.0},
                    new Object[]{1514764800000L, 8L, 1.0,8.0,1.0,1.0},
                    new Object[]{1577836800000L, 8L, 4.0,32.0,4.0,4.0});


            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 32, assertMinutesInfo.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertMinutes2Info.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 32, assertHoursInfo.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertHours2Info.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 16, assertDaysInfo.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 32, assertDays2Info.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 8, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 16, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 4, assertYearsInfo.eventCount, 60000);
            log.info("Wait over for: years - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 8, assertYearsInfo.eventCount, 60000);
            log.info("Wait over for: years - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutesInfo.events, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutes2Info.events, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHoursInfo.events, expectedHours), "hours - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHours2Info.events, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDaysInfo.events, expectedDays), "days - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDays2Info.events, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonthsInfo.events, expectedMonths), "months - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonths2Info.events, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertYearsInfo.events, expectedYears), "years - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertYears2Info.events, expectedYears2), "years - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> minutesRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MINUTES"));
            List<Object[]> minutes2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MINUTES"));
            List<Object[]> hoursRecords = runStatementAndGetResult(generateAggregatedAssertQuery("HOURS"));
            List<Object[]> hours2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("HOURS"));
            List<Object[]> daysRecords = runStatementAndGetResult(generateAggregatedAssertQuery("DAYS"));
            List<Object[]> days2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("DAYS"));
            List<Object[]> monthsRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MONTHS"));
            List<Object[]> months2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MONTHS"));
            List<Object[]> yearsRecords = runStatementAndGetResult(generateAggregatedAssertQuery("YEARS"));
            List<Object[]> years2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("YEARS"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutesRecords, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(minutes2Records, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hoursRecords, expectedHours), "hours - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hours2Records, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(daysRecords, expectedDays), "days - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(days2Records, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(monthsRecords, expectedMonths), "months - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(months2Records, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(yearsRecords, expectedYears), "years - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(years2Records, expectedYears2), "years - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("MINUTES");
            assertTimestamps("HOURS");
            assertTimestamps("DAYS");
            assertTimestamps("MONTHS");
            assertTimestamps("YEARS");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    // Starting from min [END]

    // Starting from hour [BEGIN]

//    @Test // TODO Passes
    public void testContinuousHourToDay() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousHourToDay");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertHoursStream(startTime string, endTime string);\n" +
                        "define stream AssertDaysStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    count() as totalRequestCount,\n" +
                        "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                        "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every hour...day;\n" +


                        /* Per hours */
                        // Group by: context
                        "\n@info(name = 'assertHours')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertHours2')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per days */
                        // Group by: context
                        "\n@info(name = 'assertDays')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertDays2')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert hours
            AssertionInfoHolder assertHoursInfo = new AssertionInfoHolder();
            addCallback("assertHours", siddhiAppRuntime, assertHoursInfo);
            AssertionInfoHolder assertHours2Info = new AssertionInfoHolder();
            addCallback("assertHours2", siddhiAppRuntime, assertHours2Info);

            // Info to assert days
            AssertionInfoHolder assertDaysInfo = new AssertionInfoHolder();
            addCallback("assertDays", siddhiAppRuntime, assertDaysInfo);
            AssertionInfoHolder assertDays2Info = new AssertionInfoHolder();
            addCallback("assertDays2", siddhiAppRuntime, assertDays2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertHoursStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertHoursStream");
            InputHandler assertDaysStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertDaysStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.HOURS);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertHoursStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertDaysStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events

            List<Object[]> expectedHours = Arrays.asList(
                    new Object[]{1591074000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593666000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590984000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527915600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591070400000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530417600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedHours2 = Arrays.asList(
                    new Object[]{1591074000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593576000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530504000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527915600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527829200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593666000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1590987600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527912000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 3.0,3.0,3.0,3.0});

            List<Object[]> expectedDays = Arrays.asList(
                    new Object[]{1590969600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1530489600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593648000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1590969600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593561600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530489600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593648000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 1.5,6.0,1.0,2.0});

            List<Object[]> expectedDays2 = Arrays.asList(
                    new Object[]{1590969600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530489600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527811200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593648000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593561600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1527897600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593561600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527811200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530489600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593561600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1530489600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1591056000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1593561600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527897600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1590969600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527897600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593648000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1530489600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527897600000L, 2L, 4.0,8.0,4.0,4.0});


            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 32, assertHoursInfo.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertHours2Info.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 16, assertDaysInfo.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 32, assertDays2Info.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHoursInfo.events, expectedHours), "hours - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHours2Info.events, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDaysInfo.events, expectedDays), "days - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDays2Info.events, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> hoursRecords = runStatementAndGetResult(generateAggregatedAssertQuery("HOURS"));
            List<Object[]> hours2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("HOURS"));
            List<Object[]> daysRecords = runStatementAndGetResult(generateAggregatedAssertQuery("DAYS"));
            List<Object[]> days2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("DAYS"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hoursRecords, expectedHours), "hours - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hours2Records, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(daysRecords, expectedDays), "days - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(days2Records, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("HOURS");
            assertTimestamps("DAYS");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

//    @Test // TODO Passes
    public void testContinuousHourToMonth() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousHourToMonth");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertHoursStream(startTime string, endTime string);\n" +
                        "define stream AssertDaysStream(startTime string, endTime string);\n" +
                        "define stream AssertMonthsStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    count() as totalRequestCount,\n" +
                        "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                        "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every hour...month;\n" +


                        /* Per hours */
                        // Group by: context
                        "\n@info(name = 'assertHours')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertHours2')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per days */
                        // Group by: context
                        "\n@info(name = 'assertDays')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertDays2')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per months */
                        // Group by: context
                        "\n@info(name = 'assertMonths')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMonths2')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert hours
            AssertionInfoHolder assertHoursInfo = new AssertionInfoHolder();
            addCallback("assertHours", siddhiAppRuntime, assertHoursInfo);
            AssertionInfoHolder assertHours2Info = new AssertionInfoHolder();
            addCallback("assertHours2", siddhiAppRuntime, assertHours2Info);

            // Info to assert days
            AssertionInfoHolder assertDaysInfo = new AssertionInfoHolder();
            addCallback("assertDays", siddhiAppRuntime, assertDaysInfo);
            AssertionInfoHolder assertDays2Info = new AssertionInfoHolder();
            addCallback("assertDays2", siddhiAppRuntime, assertDays2Info);

            // Info to assert months
            AssertionInfoHolder assertMonthsInfo = new AssertionInfoHolder();
            addCallback("assertMonths", siddhiAppRuntime, assertMonthsInfo);
            AssertionInfoHolder assertMonths2Info = new AssertionInfoHolder();
            addCallback("assertMonths2", siddhiAppRuntime, assertMonths2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertHoursStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertHoursStream");
            InputHandler assertDaysStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertDaysStream");
            InputHandler assertMonthsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMonthsStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.HOURS);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertHoursStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertDaysStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertMonthsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events

            List<Object[]> expectedHours = Arrays.asList(
                    new Object[]{1591074000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593666000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590984000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527915600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591070400000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530417600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedHours2 = Arrays.asList(
                    new Object[]{1591074000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593576000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530504000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527915600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527829200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593666000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1590987600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527912000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 3.0,3.0,3.0,3.0});

            List<Object[]> expectedDays = Arrays.asList(
                    new Object[]{1590969600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1530489600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593648000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1590969600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593561600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530489600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593648000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 1.5,6.0,1.0,2.0});

            List<Object[]> expectedDays2 = Arrays.asList(
                    new Object[]{1590969600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530489600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527811200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593648000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593561600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1527897600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593561600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527811200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530489600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593561600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1530489600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1591056000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1593561600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527897600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1590969600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527897600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593648000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1530489600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527897600000L, 2L, 4.0,8.0,4.0,4.0});

            List<Object[]> expectedMonths = Arrays.asList(
                    new Object[]{1530403200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1590969600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1527811200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1593561600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1530403200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1527811200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1593561600000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1590969600000L, 8L, 1.5,12.0,1.0,2.0});

            List<Object[]> expectedMonths2 = Arrays.asList(
                    new Object[]{1530403200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1593561600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1530403200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1527811200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1593561600000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1590969600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1593561600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1527811200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1527811200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1590969600000L, 4L, 2.0,8.0,2.0,2.0});


            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 32, assertHoursInfo.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertHours2Info.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 16, assertDaysInfo.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 32, assertDays2Info.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 8, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 16, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHoursInfo.events, expectedHours), "hours - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHours2Info.events, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDaysInfo.events, expectedDays), "days - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDays2Info.events, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonthsInfo.events, expectedMonths), "months - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonths2Info.events, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> hoursRecords = runStatementAndGetResult(generateAggregatedAssertQuery("HOURS"));
            List<Object[]> hours2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("HOURS"));
            List<Object[]> daysRecords = runStatementAndGetResult(generateAggregatedAssertQuery("DAYS"));
            List<Object[]> days2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("DAYS"));
            List<Object[]> monthsRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MONTHS"));
            List<Object[]> months2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MONTHS"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hoursRecords, expectedHours), "hours - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hours2Records, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(daysRecords, expectedDays), "days - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(days2Records, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(monthsRecords, expectedMonths), "months - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(months2Records, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("HOURS");
            assertTimestamps("DAYS");
            assertTimestamps("MONTHS");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

//    @Test // TODO Passes
    public void testContinuousHourToYear() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousHourToYear");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertHoursStream(startTime string, endTime string);\n" +
                        "define stream AssertDaysStream(startTime string, endTime string);\n" +
                        "define stream AssertMonthsStream(startTime string, endTime string);\n" +
                        "define stream AssertYearsStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    count() as totalRequestCount,\n" +
                        "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                        "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every hour...year;\n" +


                        /* Per hours */
                        // Group by: context
                        "\n@info(name = 'assertHours')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertHours2')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per days */
                        // Group by: context
                        "\n@info(name = 'assertDays')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertDays2')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per months */
                        // Group by: context
                        "\n@info(name = 'assertMonths')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMonths2')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per years */
                        // Group by: context
                        "\n@info(name = 'assertYears')\n" +
                        "from AssertYearsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"years\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertYears2')\n" +
                        "from AssertYearsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"years\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert hours
            AssertionInfoHolder assertHoursInfo = new AssertionInfoHolder();
            addCallback("assertHours", siddhiAppRuntime, assertHoursInfo);
            AssertionInfoHolder assertHours2Info = new AssertionInfoHolder();
            addCallback("assertHours2", siddhiAppRuntime, assertHours2Info);

            // Info to assert days
            AssertionInfoHolder assertDaysInfo = new AssertionInfoHolder();
            addCallback("assertDays", siddhiAppRuntime, assertDaysInfo);
            AssertionInfoHolder assertDays2Info = new AssertionInfoHolder();
            addCallback("assertDays2", siddhiAppRuntime, assertDays2Info);

            // Info to assert months
            AssertionInfoHolder assertMonthsInfo = new AssertionInfoHolder();
            addCallback("assertMonths", siddhiAppRuntime, assertMonthsInfo);
            AssertionInfoHolder assertMonths2Info = new AssertionInfoHolder();
            addCallback("assertMonths2", siddhiAppRuntime, assertMonths2Info);

            // Info to assert years
            AssertionInfoHolder assertYearsInfo = new AssertionInfoHolder();
            addCallback("assertYears", siddhiAppRuntime, assertYearsInfo);
            AssertionInfoHolder assertYears2Info = new AssertionInfoHolder();
            addCallback("assertYears2", siddhiAppRuntime, assertYears2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertHoursStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertHoursStream");
            InputHandler assertDaysStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertDaysStream");
            InputHandler assertMonthsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMonthsStream");
            InputHandler assertYearsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertYearsStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.HOURS);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertHoursStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertDaysStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertMonthsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertYearsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events

            List<Object[]> expectedHours = Arrays.asList(
                    new Object[]{1591074000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591070400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527912000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590984000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593576000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527915600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530417600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530504000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593666000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527829200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593579600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527829200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1593579600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593576000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530421200000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1527825600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530504000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1590984000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530421200000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527915600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527825600000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1530507600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593666000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1591070400000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1530417600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1591074000000L, 2L, 1.5,3.0,1.0,2.0},
                    new Object[]{1590987600000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1527912000000L, 2L, 3.5,7.0,3.0,4.0},
                    new Object[]{1593662400000L, 2L, 3.5,7.0,3.0,4.0});

            List<Object[]> expectedHours2 = Arrays.asList(
                    new Object[]{1591074000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1591070400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593576000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527915600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1530417600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530504000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593666000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593579600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527829200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530507600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530504000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591074000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530421200000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1590987600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527912000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1530421200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527829200000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1593579600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527825600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527915600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527829200000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593579600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530507600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593666000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527915600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593666000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530417600000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590984000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1590984000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593576000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1591074000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1527825600000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1590987600000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1527912000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1593666000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1590987600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593576000000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530421200000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1593662400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530417600000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1591070400000L, 1L, 3.0,3.0,3.0,3.0},
                    new Object[]{1530504000000L, 1L, 1.0,1.0,1.0,1.0},
                    new Object[]{1591074000000L, 1L, 2.0,2.0,2.0,2.0},
                    new Object[]{1527912000000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1593662400000L, 1L, 4.0,4.0,4.0,4.0},
                    new Object[]{1530507600000L, 1L, 3.0,3.0,3.0,3.0});

            List<Object[]> expectedDays = Arrays.asList(
                    new Object[]{1590969600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1530489600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593648000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1590969600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593561600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530489600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593648000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 1.5,6.0,1.0,2.0});

            List<Object[]> expectedDays2 = Arrays.asList(
                    new Object[]{1590969600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530489600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527811200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593648000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593561600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1527897600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593561600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527811200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530489600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593561600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1530489600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1591056000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1593561600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527897600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1590969600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527897600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593648000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1530489600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527897600000L, 2L, 4.0,8.0,4.0,4.0});

            List<Object[]> expectedMonths = Arrays.asList(
                    new Object[]{1530403200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1590969600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1527811200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1593561600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1530403200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1527811200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1593561600000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1590969600000L, 8L, 1.5,12.0,1.0,2.0});

            List<Object[]> expectedMonths2 = Arrays.asList(
                    new Object[]{1530403200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1593561600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1530403200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1527811200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1593561600000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1590969600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1593561600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1527811200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1527811200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1590969600000L, 4L, 2.0,8.0,2.0,2.0});

            List<Object[]> expectedYears = Arrays.asList(
                    new Object[]{1514764800000L, 16L, 1.5,24.0,1.0,2.0},
                    new Object[]{1577836800000L, 16L, 1.5,24.0,1.0,2.0},
                    new Object[]{1514764800000L, 16L, 3.5,56.0,3.0,4.0},
                    new Object[]{1577836800000L, 16L, 3.5,56.0,3.0,4.0});

            List<Object[]> expectedYears2 = Arrays.asList(
                    new Object[]{1514764800000L, 8L, 2.0,16.0,2.0,2.0},
                    new Object[]{1577836800000L, 8L, 1.0,8.0,1.0,1.0},
                    new Object[]{1514764800000L, 8L, 4.0,32.0,4.0,4.0},
                    new Object[]{1577836800000L, 8L, 2.0,16.0,2.0,2.0},
                    new Object[]{1514764800000L, 8L, 3.0,24.0,3.0,3.0},
                    new Object[]{1577836800000L, 8L, 3.0,24.0,3.0,3.0},
                    new Object[]{1514764800000L, 8L, 1.0,8.0,1.0,1.0},
                    new Object[]{1577836800000L, 8L, 4.0,32.0,4.0,4.0});


            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 32, assertHoursInfo.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertHours2Info.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 16, assertDaysInfo.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 32, assertDays2Info.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 8, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 16, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 4, assertYearsInfo.eventCount, 60000);
            log.info("Wait over for: years - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 8, assertYearsInfo.eventCount, 60000);
            log.info("Wait over for: years - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHoursInfo.events, expectedHours), "hours - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHours2Info.events, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDaysInfo.events, expectedDays), "days - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDays2Info.events, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonthsInfo.events, expectedMonths), "months - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonths2Info.events, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertYearsInfo.events, expectedYears), "years - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertYears2Info.events, expectedYears2), "years - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> hoursRecords = runStatementAndGetResult(generateAggregatedAssertQuery("HOURS"));
            List<Object[]> hours2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("HOURS"));
            List<Object[]> daysRecords = runStatementAndGetResult(generateAggregatedAssertQuery("DAYS"));
            List<Object[]> days2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("DAYS"));
            List<Object[]> monthsRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MONTHS"));
            List<Object[]> months2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MONTHS"));
            List<Object[]> yearsRecords = runStatementAndGetResult(generateAggregatedAssertQuery("YEARS"));
            List<Object[]> years2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("YEARS"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hoursRecords, expectedHours), "hours - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(hours2Records, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(daysRecords, expectedDays), "days - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(days2Records, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(monthsRecords, expectedMonths), "months - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(months2Records, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(yearsRecords, expectedYears), "years - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(years2Records, expectedYears2), "years - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("HOURS");
            assertTimestamps("DAYS");
            assertTimestamps("MONTHS");
            assertTimestamps("YEARS");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    // Starting from hour [END]

    // Starting from day [BEGIN]

//    @Test // TODO Passes
    public void testContinuousDayToMonth() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousDayToMonth");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertDaysStream(startTime string, endTime string);\n" +
                        "define stream AssertMonthsStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    count() as totalRequestCount,\n" +
                        "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                        "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every day...month;\n" +


                        /* Per days */
                        // Group by: context
                        "\n@info(name = 'assertDays')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertDays2')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per months */
                        // Group by: context
                        "\n@info(name = 'assertMonths')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMonths2')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert days
            AssertionInfoHolder assertDaysInfo = new AssertionInfoHolder();
            addCallback("assertDays", siddhiAppRuntime, assertDaysInfo);
            AssertionInfoHolder assertDays2Info = new AssertionInfoHolder();
            addCallback("assertDays2", siddhiAppRuntime, assertDays2Info);

            // Info to assert months
            AssertionInfoHolder assertMonthsInfo = new AssertionInfoHolder();
            addCallback("assertMonths", siddhiAppRuntime, assertMonthsInfo);
            AssertionInfoHolder assertMonths2Info = new AssertionInfoHolder();
            addCallback("assertMonths2", siddhiAppRuntime, assertMonths2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertDaysStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertDaysStream");
            InputHandler assertMonthsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMonthsStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.HOURS);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertDaysStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertMonthsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events

            List<Object[]> expectedDays = Arrays.asList(
                    new Object[]{1590969600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1530489600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593648000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1590969600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593561600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530489600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593648000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 1.5,6.0,1.0,2.0});

            List<Object[]> expectedDays2 = Arrays.asList(
                    new Object[]{1590969600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530489600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527811200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593648000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593561600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1527897600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593561600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527811200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530489600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593561600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1530489600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1591056000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1593561600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527897600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1590969600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527897600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593648000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1530489600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527897600000L, 2L, 4.0,8.0,4.0,4.0});

            List<Object[]> expectedMonths = Arrays.asList(
                    new Object[]{1530403200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1590969600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1527811200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1593561600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1530403200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1527811200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1593561600000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1590969600000L, 8L, 1.5,12.0,1.0,2.0});

            List<Object[]> expectedMonths2 = Arrays.asList(
                    new Object[]{1530403200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1593561600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1530403200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1527811200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1593561600000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1590969600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1593561600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1527811200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1527811200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1590969600000L, 4L, 2.0,8.0,2.0,2.0});


            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 16, assertDaysInfo.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 32, assertDays2Info.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 8, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 16, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDaysInfo.events, expectedDays), "days - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDays2Info.events, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonthsInfo.events, expectedMonths), "months - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonths2Info.events, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> daysRecords = runStatementAndGetResult(generateAggregatedAssertQuery("DAYS"));
            List<Object[]> days2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("DAYS"));
            List<Object[]> monthsRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MONTHS"));
            List<Object[]> months2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MONTHS"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(daysRecords, expectedDays), "days - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(days2Records, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(monthsRecords, expectedMonths), "months - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(months2Records, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("DAYS");
            assertTimestamps("MONTHS");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

//    @Test // TODO Passes
    public void testContinuousDayToYear() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousDayToYear");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertDaysStream(startTime string, endTime string);\n" +
                        "define stream AssertMonthsStream(startTime string, endTime string);\n" +
                        "define stream AssertYearsStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    count() as totalRequestCount,\n" +
                        "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                        "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every hour...year;\n" +


                        /* Per days */
                        // Group by: context
                        "\n@info(name = 'assertDays')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertDays2')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per months */
                        // Group by: context
                        "\n@info(name = 'assertMonths')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMonths2')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per years */
                        // Group by: context
                        "\n@info(name = 'assertYears')\n" +
                        "from AssertYearsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"years\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertYears2')\n" +
                        "from AssertYearsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"years\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert days
            AssertionInfoHolder assertDaysInfo = new AssertionInfoHolder();
            addCallback("assertDays", siddhiAppRuntime, assertDaysInfo);
            AssertionInfoHolder assertDays2Info = new AssertionInfoHolder();
            addCallback("assertDays2", siddhiAppRuntime, assertDays2Info);

            // Info to assert months
            AssertionInfoHolder assertMonthsInfo = new AssertionInfoHolder();
            addCallback("assertMonths", siddhiAppRuntime, assertMonthsInfo);
            AssertionInfoHolder assertMonths2Info = new AssertionInfoHolder();
            addCallback("assertMonths2", siddhiAppRuntime, assertMonths2Info);

            // Info to assert years
            AssertionInfoHolder assertYearsInfo = new AssertionInfoHolder();
            addCallback("assertYears", siddhiAppRuntime, assertYearsInfo);
            AssertionInfoHolder assertYears2Info = new AssertionInfoHolder();
            addCallback("assertYears2", siddhiAppRuntime, assertYears2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertDaysStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertDaysStream");
            InputHandler assertMonthsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMonthsStream");
            InputHandler assertYearsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertYearsStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.HOURS);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertDaysStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertMonthsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertYearsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events

            List<Object[]> expectedDays = Arrays.asList(
                    new Object[]{1590969600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530403200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1530489600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593648000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1590969600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1593561600000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1530489600000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1591056000000L, 4L, 3.5,14.0,3.0,4.0},
                    new Object[]{1593648000000L, 4L, 1.5,6.0,1.0,2.0},
                    new Object[]{1527897600000L, 4L, 1.5,6.0,1.0,2.0});

            List<Object[]> expectedDays2 = Arrays.asList(
                    new Object[]{1590969600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530403200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530489600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527811200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593648000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593561600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1527897600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593561600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527811200000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1530489600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1593561600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1590969600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1530489600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527811200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1591056000000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1593561600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1593648000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1527897600000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1590969600000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1527897600000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1593648000000L, 2L, 2.0,4.0,2.0,2.0},
                    new Object[]{1591056000000L, 2L, 1.0,2.0,1.0,1.0},
                    new Object[]{1530403200000L, 2L, 3.0,6.0,3.0,3.0},
                    new Object[]{1530489600000L, 2L, 4.0,8.0,4.0,4.0},
                    new Object[]{1527897600000L, 2L, 4.0,8.0,4.0,4.0});

            List<Object[]> expectedMonths = Arrays.asList(
                    new Object[]{1530403200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1590969600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1527811200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1593561600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1530403200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1527811200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1593561600000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1590969600000L, 8L, 1.5,12.0,1.0,2.0});

            List<Object[]> expectedMonths2 = Arrays.asList(
                    new Object[]{1530403200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1593561600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1530403200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1527811200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1593561600000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1590969600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1593561600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1527811200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1527811200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1590969600000L, 4L, 2.0,8.0,2.0,2.0});

            List<Object[]> expectedYears = Arrays.asList(
                    new Object[]{1514764800000L, 16L, 1.5,24.0,1.0,2.0},
                    new Object[]{1577836800000L, 16L, 1.5,24.0,1.0,2.0},
                    new Object[]{1514764800000L, 16L, 3.5,56.0,3.0,4.0},
                    new Object[]{1577836800000L, 16L, 3.5,56.0,3.0,4.0});

            List<Object[]> expectedYears2 = Arrays.asList(
                    new Object[]{1514764800000L, 8L, 2.0,16.0,2.0,2.0},
                    new Object[]{1577836800000L, 8L, 1.0,8.0,1.0,1.0},
                    new Object[]{1514764800000L, 8L, 4.0,32.0,4.0,4.0},
                    new Object[]{1577836800000L, 8L, 2.0,16.0,2.0,2.0},
                    new Object[]{1514764800000L, 8L, 3.0,24.0,3.0,3.0},
                    new Object[]{1577836800000L, 8L, 3.0,24.0,3.0,3.0},
                    new Object[]{1514764800000L, 8L, 1.0,8.0,1.0,1.0},
                    new Object[]{1577836800000L, 8L, 4.0,32.0,4.0,4.0});


            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 16, assertDaysInfo.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 32, assertDays2Info.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 8, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 16, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 4, assertYearsInfo.eventCount, 60000);
            log.info("Wait over for: years - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 8, assertYearsInfo.eventCount, 60000);
            log.info("Wait over for: years - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDaysInfo.events, expectedDays), "days - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDays2Info.events, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonthsInfo.events, expectedMonths), "months - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonths2Info.events, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertYearsInfo.events, expectedYears), "years - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertYears2Info.events, expectedYears2), "years - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> daysRecords = runStatementAndGetResult(generateAggregatedAssertQuery("DAYS"));
            List<Object[]> days2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("DAYS"));
            List<Object[]> monthsRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MONTHS"));
            List<Object[]> months2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MONTHS"));
            List<Object[]> yearsRecords = runStatementAndGetResult(generateAggregatedAssertQuery("YEARS"));
            List<Object[]> years2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("YEARS"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(daysRecords, expectedDays), "days - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(days2Records, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(monthsRecords, expectedMonths), "months - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(months2Records, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(yearsRecords, expectedYears), "years - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(years2Records, expectedYears2), "years - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("DAYS");
            assertTimestamps("MONTHS");
            assertTimestamps("YEARS");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    // Starting from day [END]

    // Starting from month [BEGIN]

//    @Test // TODO Passes
    public void testContinuousMonthToYear() throws InterruptedException, DataSourceException, SQLException {
        log.info("testContinuousMonthToYear");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertMonthsStream(startTime string, endTime string);\n" +
                        "define stream AssertYearsStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    count() as totalRequestCount,\n" +
                        "    avg(waitingTime) as avgWaitingTime, sum(waitingTime) as totalWaitingTime,\n" +
                        "    min(waitingTime) as minWaitingTime, max(waitingTime) as maxWaitingTime\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every month...year;\n" +


                        /* Per months */
                        // Group by: context
                        "\n@info(name = 'assertMonths')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMonths2')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per years */
                        // Group by: context
                        "\n@info(name = 'assertYears')\n" +
                        "from AssertYearsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"years\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertYears2')\n" +
                        "from AssertYearsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"years\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount,\n" +
                        "    avg(avgWaitingTime) as avgWaitingTime, sum(totalWaitingTime) as totalWaitingTime,\n" +
                        "    min(minWaitingTime) as minWaitingTime, max(maxWaitingTime) as maxWaitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert months
            AssertionInfoHolder assertMonthsInfo = new AssertionInfoHolder();
            addCallback("assertMonths", siddhiAppRuntime, assertMonthsInfo);
            AssertionInfoHolder assertMonths2Info = new AssertionInfoHolder();
            addCallback("assertMonths2", siddhiAppRuntime, assertMonths2Info);

            // Info to assert years
            AssertionInfoHolder assertYearsInfo = new AssertionInfoHolder();
            addCallback("assertYears", siddhiAppRuntime, assertYearsInfo);
            AssertionInfoHolder assertYears2Info = new AssertionInfoHolder();
            addCallback("assertYears2", siddhiAppRuntime, assertYears2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertMonthsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMonthsStream");
            InputHandler assertYearsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertYearsStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.HOURS);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertMonthsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertYearsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events

            List<Object[]> expectedMonths = Arrays.asList(
                    new Object[]{1530403200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1590969600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1527811200000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1593561600000L, 8L, 3.5,28.0,3.0,4.0},
                    new Object[]{1530403200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1527811200000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1593561600000L, 8L, 1.5,12.0,1.0,2.0},
                    new Object[]{1590969600000L, 8L, 1.5,12.0,1.0,2.0});

            List<Object[]> expectedMonths2 = Arrays.asList(
                    new Object[]{1530403200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1527811200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1593561600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1530403200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1527811200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1593561600000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1590969600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1593561600000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1590969600000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1527811200000L, 4L, 4.0,16.0,4.0,4.0},
                    new Object[]{1593561600000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 2.0,8.0,2.0,2.0},
                    new Object[]{1527811200000L, 4L, 1.0,4.0,1.0,1.0},
                    new Object[]{1530403200000L, 4L, 3.0,12.0,3.0,3.0},
                    new Object[]{1590969600000L, 4L, 2.0,8.0,2.0,2.0});

            List<Object[]> expectedYears = Arrays.asList(
                    new Object[]{1514764800000L, 16L, 1.5,24.0,1.0,2.0},
                    new Object[]{1577836800000L, 16L, 1.5,24.0,1.0,2.0},
                    new Object[]{1514764800000L, 16L, 3.5,56.0,3.0,4.0},
                    new Object[]{1577836800000L, 16L, 3.5,56.0,3.0,4.0});

            List<Object[]> expectedYears2 = Arrays.asList(
                    new Object[]{1514764800000L, 8L, 2.0,16.0,2.0,2.0},
                    new Object[]{1577836800000L, 8L, 1.0,8.0,1.0,1.0},
                    new Object[]{1514764800000L, 8L, 4.0,32.0,4.0,4.0},
                    new Object[]{1577836800000L, 8L, 2.0,16.0,2.0,2.0},
                    new Object[]{1514764800000L, 8L, 3.0,24.0,3.0,3.0},
                    new Object[]{1577836800000L, 8L, 3.0,24.0,3.0,3.0},
                    new Object[]{1514764800000L, 8L, 1.0,8.0,1.0,1.0},
                    new Object[]{1577836800000L, 8L, 4.0,32.0,4.0,4.0});


            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 8, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 16, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 4, assertYearsInfo.eventCount, 60000);
            log.info("Wait over for: years - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 8, assertYearsInfo.eventCount, 60000);
            log.info("Wait over for: years - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonthsInfo.events, expectedMonths), "months - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonths2Info.events, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertYearsInfo.events, expectedYears), "years - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertYears2Info.events, expectedYears2), "years - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");

            // Expected records from table
            List<Object[]> monthsRecords = runStatementAndGetResult(generateAggregatedAssertQuery("MONTHS"));
            List<Object[]> months2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("MONTHS"));
            List<Object[]> yearsRecords = runStatementAndGetResult(generateAggregatedAssertQuery("YEARS"));
            List<Object[]> years2Records = runStatementAndGetResult(generateAggregatedAssertQuery2("YEARS"));

            // Assert with events got from database table
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(monthsRecords, expectedMonths), "months - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(months2Records, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(yearsRecords, expectedYears), "years - group by AGG_TIMESTAMP, context matched against database table");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(years2Records, expectedYears2), "years - group by AGG_TIMESTAMP, context, version matched against database table");

            // Assert timestamps from database tables
            assertTimestamps("MONTHS");
            assertTimestamps("YEARS");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    // Starting from month [END]

//    @Test // TODO Passes
    public void testWithNonAggregatedAttribute() throws InterruptedException, DataSourceException, SQLException { // TODO write after anusha's fix
        log.info("testWithNonAggregatedAttribute");
        SiddhiManager siddhiManager = StreamProcessorDataHolder.getSiddhiManager();
        String siddhiAppBody =
                "define stream RequestStream(context string, version string, userId string, waitingTime double, requestTimestamp long);\n" +
                        "define stream AssertMinutesStream(startTime string, endTime string);\n" +
                        "define stream AssertHoursStream(startTime string, endTime string);\n" +
                        "define stream AssertDaysStream(startTime string, endTime string);\n" +
                        "define stream AssertMonthsStream(startTime string, endTime string);\n" +
                        "define stream AssertYearsStream(startTime string, endTime string);\n" +

//                "@store(type = 'rdbms', datasource = 'WSO2AM_STATS_DB', field.length = \"hostName:200, userId:150, tenantDomain:150\")\n" +
//                "@purge(enable = 'false')\n" +
                        getStoreAnnotation() +
                        "define aggregation REQUEST_SUMMARY \n" +
                        "from RequestStream\n" +
                        "select \n" +
                        "    requestTimestamp, context, version, userId, \n" +
                        "    waitingTime,\n" + // TODO this is the un-aggregated attribute
                        "    count() as totalRequestCount\n" +
                        "group by context, version, userId\n" +
                        "aggregate by requestTimestamp every sec...year;\n" +


                        /* Per minutes */
                        // Group by: context
                        "\n@info(name = 'assertMinutes')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount, waitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMinutes2')\n" +
                        "from AssertMinutesStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"minutes\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount, waitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per hours */
                        // Group by: context
                        "\n@info(name = 'assertHours')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount, waitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertHours2')\n" +
                        "from AssertHoursStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"hours\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount, waitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per days */
                        // Group by: context
                        "\n@info(name = 'assertDays')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount, waitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertDays2')\n" +
                        "from AssertDaysStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"days\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount, waitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per months */
                        // Group by: context
                        "\n@info(name = 'assertMonths')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount, waitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertMonths2')\n" +
                        "from AssertMonthsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"months\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount, waitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +


                        /* Per years */
                        // Group by: context
                        "\n@info(name = 'assertYears')\n" +
                        "from AssertYearsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"years\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount, waitingTime\n" +
                        "group by AGG_TIMESTAMP, context\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        // Group by: context, version
                        "\n@info(name = 'assertYears2')\n" +
                        "from AssertYearsStream as S join REQUEST_SUMMARY as A\n" +
                        "    within startTime, endTime\n" +
                        "    per \"years\"\n" +
                        "select AGG_TIMESTAMP, sum(totalRequestCount) as totalRequestCount, waitingTime\n" +
                        "group by AGG_TIMESTAMP, context, version\n" +
                        "insert into AssertOutputDiscardStream;\n" +

                        "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppBody);

        try {
            // Info to assert minutes
            AssertionInfoHolder assertMinutesInfo = new AssertionInfoHolder();
            addCallback("assertMinutes", siddhiAppRuntime, assertMinutesInfo);
            AssertionInfoHolder assertMinutes2Info = new AssertionInfoHolder();
            addCallback("assertMinutes2", siddhiAppRuntime, assertMinutes2Info);

            // Info to assert hours
            AssertionInfoHolder assertHoursInfo = new AssertionInfoHolder();
            addCallback("assertHours", siddhiAppRuntime, assertHoursInfo);
            AssertionInfoHolder assertHours2Info = new AssertionInfoHolder();
            addCallback("assertHours2", siddhiAppRuntime, assertHours2Info);

            // Info to assert days
            AssertionInfoHolder assertDaysInfo = new AssertionInfoHolder();
            addCallback("assertDays", siddhiAppRuntime, assertDaysInfo);
            AssertionInfoHolder assertDays2Info = new AssertionInfoHolder();
            addCallback("assertDays2", siddhiAppRuntime, assertDays2Info);

            // Info to assert months
            AssertionInfoHolder assertMonthsInfo = new AssertionInfoHolder();
            addCallback("assertMonths", siddhiAppRuntime, assertMonthsInfo);
            AssertionInfoHolder assertMonths2Info = new AssertionInfoHolder();
            addCallback("assertMonths2", siddhiAppRuntime, assertMonths2Info);

            // Info to assert years
            AssertionInfoHolder assertYearsInfo = new AssertionInfoHolder();
            addCallback("assertYears", siddhiAppRuntime, assertYearsInfo);
            AssertionInfoHolder assertYears2Info = new AssertionInfoHolder();
            addCallback("assertYears2", siddhiAppRuntime, assertYears2Info);

            // Get stream input handlers
            InputHandler requestStreamInputHandler = siddhiAppRuntime.getInputHandler("RequestStream");
            InputHandler assertMinutesStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMinutesStream");
            InputHandler assertHoursStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertHoursStream");
            InputHandler assertDaysStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertDaysStream");
            InputHandler assertMonthsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertMonthsStream");
            InputHandler assertYearsStreamInputHandler = siddhiAppRuntime.getInputHandler("AssertYearsStream");

            siddhiAppRuntime.start();

            // Send events for aggregation
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 1.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:01 GMT+0:00")}); // y1 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 2.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:00:59 GMT+0:00")}); // y1 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 3.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:01 GMT+0:00")}); // y1 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 4.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 4:01:59 GMT+0:00")}); // y1 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 5.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:01 GMT+0:00")}); // y1 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 6.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:00:59 GMT+0:00")}); // y1 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 7.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:01 GMT+0:00")}); // y1 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 8.0, HybridAggregationTestUtils.convertToEpoch("2018-06-01 5:01:59 GMT+0:00")}); // y1 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 9.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:01 GMT+0:00")}); // y1 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 10.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:00:59 GMT+0:00")}); // y1 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 11.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:01 GMT+0:00")}); // y1 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 12.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 4:01:59 GMT+0:00")}); // y1 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 13.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:01 GMT+0:00")}); // y1 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 14.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:00:59 GMT+0:00")}); // y1 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 15.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:01 GMT+0:00")}); // y1 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 16.0, HybridAggregationTestUtils.convertToEpoch("2018-06-02 5:01:59 GMT+0:00")}); // y1 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 17.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:01 GMT+0:00")}); // y1 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 18.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:00:59 GMT+0:00")}); // y1 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 19.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:01 GMT+0:00")}); // y1 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 20.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 4:01:59 GMT+0:00")}); // y1 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 21.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:01 GMT+0:00")}); // y1 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 22.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:00:59 GMT+0:00")}); // y1 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 23.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:01 GMT+0:00")}); // y1 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 24.0, HybridAggregationTestUtils.convertToEpoch("2018-07-01 5:01:59 GMT+0:00")}); // y1 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 25.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:01 GMT+0:00")}); // y1 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 26.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:00:59 GMT+0:00")}); // y1 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 27.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:01 GMT+0:00")}); // y1 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 28.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 4:01:59 GMT+0:00")}); // y1 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 29.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:01 GMT+0:00")}); // y1 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 30.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:00:59 GMT+0:00")}); // y1 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 31.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:01 GMT+0:00")}); // y1 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 32.0, HybridAggregationTestUtils.convertToEpoch("2018-07-02 5:01:59 GMT+0:00")}); // y1 M2 d2 H2 m2 s2

            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 33.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:01 GMT+0:00")}); // y2 M1 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 34.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:00:59 GMT+0:00")}); // y2 M1 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 35.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:01 GMT+0:00")}); // y2 M1 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 36.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 4:01:59 GMT+0:00")}); // y2 M1 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 37.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:01 GMT+0:00")}); // y2 M1 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 38.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:00:59 GMT+0:00")}); // y2 M1 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 39.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:01 GMT+0:00")}); // y2 M1 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 40.0, HybridAggregationTestUtils.convertToEpoch("2020-06-01 5:01:59 GMT+0:00")}); // y2 M1 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 41.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:01 GMT+0:00")}); // y2 M1 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 42.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:00:59 GMT+0:00")}); // y2 M1 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 43.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:01 GMT+0:00")}); // y2 M1 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 44.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 4:01:59 GMT+0:00")}); // y2 M1 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 45.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:01 GMT+0:00")}); // y2 M1 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 46.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:00:59 GMT+0:00")}); // y2 M1 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 47.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:01 GMT+0:00")}); // y2 M1 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 48.0, HybridAggregationTestUtils.convertToEpoch("2020-06-02 5:01:59 GMT+0:00")}); // y2 M1 d2 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 49.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:01 GMT+0:00")}); // y2 M2 d1 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 50.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:00:59 GMT+0:00")}); // y2 M2 d1 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 51.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:01 GMT+0:00")}); // y2 M2 d1 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 52.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 4:01:59 GMT+0:00")}); // y2 M2 d1 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 53.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:01 GMT+0:00")}); // y2 M2 d1 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 54.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:00:59 GMT+0:00")}); // y2 M2 d1 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 55.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:01 GMT+0:00")}); // y2 M2 d1 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 56.0, HybridAggregationTestUtils.convertToEpoch("2020-07-01 5:01:59 GMT+0:00")}); // y2 M2 d1 H2 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 57.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:01 GMT+0:00")}); // y2 M2 d2 H1 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 58.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:00:59 GMT+0:00")}); // y2 M2 d2 H1 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 59.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:01 GMT+0:00")}); // y2 M2 d2 H1 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 60.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 4:01:59 GMT+0:00")}); // y2 M2 d2 H1 m2 s2
            requestStreamInputHandler.send(new Object[]{"contextA", "v1", "user1", 61.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:01 GMT+0:00")}); // y2 M2 d2 H2 m1 s1
            requestStreamInputHandler.send(new Object[]{"contextA", "v2", "user1", 62.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:00:59 GMT+0:00")}); // y2 M2 d2 H2 m1 s2
            requestStreamInputHandler.send(new Object[]{"contextB", "v1", "user1", 63.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:01 GMT+0:00")}); // y2 M2 d2 H2 m2 s1
            requestStreamInputHandler.send(new Object[]{"contextB", "v2", "user1", 64.0, HybridAggregationTestUtils.convertToEpoch("2020-07-02 5:01:59 GMT+0:00")}); // y2 M2 d2 H2 m2 s2

            log.info("Sent events. Sleeping for 90000ms");
            Thread.sleep(90000);
            executeAggregationDurationExecutor(siddhiAppRuntime, TimePeriod.Duration.MINUTES);
            log.info("Executed aggregation duration executor. Sleeping for 90000ms");
            Thread.sleep(90000);

            // Send events for assertion
            assertMinutesStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertHoursStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertDaysStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertMonthsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});
            assertYearsStreamInputHandler.send(new Object[]{"2017-01-01 03:00:00", "2021-01-01 06:00:00"});

            // Expected events
            List<Object[]> expectedMinutes = Arrays.asList(
                    new Object[]{1527825600000L, 2L, 2.0},
                    new Object[]{1527825660000L, 2L, 4.0},
                    new Object[]{1527829200000L, 2L, 6.0},
                    new Object[]{1527829260000L, 2L, 8.0},
                    new Object[]{1527912000000L, 2L, 10.0},
                    new Object[]{1527912060000L, 2L, 12.0},
                    new Object[]{1527915600000L, 2L, 14.0},
                    new Object[]{1527915660000L, 2L, 16.0},
                    new Object[]{1530417600000L, 2L, 18.0},
                    new Object[]{1530417660000L, 2L, 20.0},
                    new Object[]{1530421200000L, 2L, 22.0},
                    new Object[]{1530421260000L, 2L, 24.0},
                    new Object[]{1530504000000L, 2L, 26.0},
                    new Object[]{1530504060000L, 2L, 28.0},
                    new Object[]{1530507600000L, 2L, 30.0},
                    new Object[]{1530507660000L, 2L, 32.0},
                    new Object[]{1590984000000L, 2L, 34.0},
                    new Object[]{1590984060000L, 2L, 36.0},
                    new Object[]{1590987600000L, 2L, 38.0},
                    new Object[]{1590987660000L, 2L, 40.0},
                    new Object[]{1591070400000L, 2L, 42.0},
                    new Object[]{1591070460000L, 2L, 44.0},
                    new Object[]{1591074000000L, 2L, 46.0},
                    new Object[]{1591074060000L, 2L, 48.0},
                    new Object[]{1593576000000L, 2L, 50.0},
                    new Object[]{1593576060000L, 2L, 52.0},
                    new Object[]{1593579600000L, 2L, 54.0},
                    new Object[]{1593579660000L, 2L, 56.0},
                    new Object[]{1593662400000L, 2L, 58.0},
                    new Object[]{1593662460000L, 2L, 60.0},
                    new Object[]{1593666000000L, 2L, 62.0},
                    new Object[]{1593666060000L, 2L, 64.0}
            );

            List<Object[]> expectedMinutes2 = Arrays.asList(
                    new Object[]{1527825600000L, 1L, 1.0},
                    new Object[]{1527825600000L, 1L, 2.0},
                    new Object[]{1527825660000L, 1L, 3.0},
                    new Object[]{1527825660000L, 1L, 4.0},
                    new Object[]{1527829200000L, 1L, 5.0},
                    new Object[]{1527829200000L, 1L, 6.0},
                    new Object[]{1527829260000L, 1L, 7.0},
                    new Object[]{1527829260000L, 1L, 8.0},
                    new Object[]{1527912000000L, 1L, 9.0},
                    new Object[]{1527912000000L, 1L, 10.0},
                    new Object[]{1527912060000L, 1L, 11.0},
                    new Object[]{1527912060000L, 1L, 12.0},
                    new Object[]{1527915600000L, 1L, 13.0},
                    new Object[]{1527915600000L, 1L, 14.0},
                    new Object[]{1527915660000L, 1L, 15.0},
                    new Object[]{1527915660000L, 1L, 16.0},
                    new Object[]{1530417600000L, 1L, 17.0},
                    new Object[]{1530417600000L, 1L, 18.0},
                    new Object[]{1530417660000L, 1L, 19.0},
                    new Object[]{1530417660000L, 1L, 20.0},
                    new Object[]{1530421200000L, 1L, 21.0},
                    new Object[]{1530421200000L, 1L, 22.0},
                    new Object[]{1530421260000L, 1L, 23.0},
                    new Object[]{1530421260000L, 1L, 24.0},
                    new Object[]{1530504000000L, 1L, 25.0},
                    new Object[]{1530504000000L, 1L, 26.0},
                    new Object[]{1530504060000L, 1L, 27.0},
                    new Object[]{1530504060000L, 1L, 28.0},
                    new Object[]{1530507600000L, 1L, 29.0},
                    new Object[]{1530507600000L, 1L, 30.0},
                    new Object[]{1530507660000L, 1L, 31.0},
                    new Object[]{1530507660000L, 1L, 32.0},
                    new Object[]{1590984000000L, 1L, 33.0},
                    new Object[]{1590984000000L, 1L, 34.0},
                    new Object[]{1590984060000L, 1L, 35.0},
                    new Object[]{1590984060000L, 1L, 36.0},
                    new Object[]{1590987600000L, 1L, 37.0},
                    new Object[]{1590987600000L, 1L, 38.0},
                    new Object[]{1590987660000L, 1L, 39.0},
                    new Object[]{1590987660000L, 1L, 40.0},
                    new Object[]{1591070400000L, 1L, 41.0},
                    new Object[]{1591070400000L, 1L, 42.0},
                    new Object[]{1591070460000L, 1L, 43.0},
                    new Object[]{1591070460000L, 1L, 44.0},
                    new Object[]{1591074000000L, 1L, 45.0},
                    new Object[]{1591074000000L, 1L, 46.0},
                    new Object[]{1591074060000L, 1L, 47.0},
                    new Object[]{1591074060000L, 1L, 48.0},
                    new Object[]{1593576000000L, 1L, 49.0},
                    new Object[]{1593576000000L, 1L, 50.0},
                    new Object[]{1593576060000L, 1L, 51.0},
                    new Object[]{1593576060000L, 1L, 52.0},
                    new Object[]{1593579600000L, 1L, 53.0},
                    new Object[]{1593579600000L, 1L, 54.0},
                    new Object[]{1593579660000L, 1L, 55.0},
                    new Object[]{1593579660000L, 1L, 56.0},
                    new Object[]{1593662400000L, 1L, 57.0},
                    new Object[]{1593662400000L, 1L, 58.0},
                    new Object[]{1593662460000L, 1L, 59.0},
                    new Object[]{1593662460000L, 1L, 60.0},
                    new Object[]{1593666000000L, 1L, 61.0},
                    new Object[]{1593666000000L, 1L, 62.0},
                    new Object[]{1593666060000L, 1L, 63.0},
                    new Object[]{1593666060000L, 1L, 64.0}
            );

            List<Object[]> expectedHours = Arrays.asList(
                    new Object[]{1527825600000L, 2L, 2.0},
                    new Object[]{1527825600000L, 2L, 4.0},
                    new Object[]{1527829200000L, 2L, 6.0},
                    new Object[]{1527829200000L, 2L, 8.0},
                    new Object[]{1527912000000L, 2L, 10.0},
                    new Object[]{1527912000000L, 2L, 12.0},
                    new Object[]{1527915600000L, 2L, 14.0},
                    new Object[]{1527915600000L, 2L, 16.0},
                    new Object[]{1530417600000L, 2L, 18.0},
                    new Object[]{1530417600000L, 2L, 20.0},
                    new Object[]{1530421200000L, 2L, 22.0},
                    new Object[]{1530421200000L, 2L, 24.0},
                    new Object[]{1530504000000L, 2L, 26.0},
                    new Object[]{1530504000000L, 2L, 28.0},
                    new Object[]{1530507600000L, 2L, 30.0},
                    new Object[]{1530507600000L, 2L, 32.0},
                    new Object[]{1590984000000L, 2L, 34.0},
                    new Object[]{1590984000000L, 2L, 36.0},
                    new Object[]{1590987600000L, 2L, 38.0},
                    new Object[]{1590987600000L, 2L, 40.0},
                    new Object[]{1591070400000L, 2L, 42.0},
                    new Object[]{1591070400000L, 2L, 44.0},
                    new Object[]{1591074000000L, 2L, 46.0},
                    new Object[]{1591074000000L, 2L, 48.0},
                    new Object[]{1593576000000L, 2L, 50.0},
                    new Object[]{1593576000000L, 2L, 52.0},
                    new Object[]{1593579600000L, 2L, 54.0},
                    new Object[]{1593579600000L, 2L, 56.0},
                    new Object[]{1593662400000L, 2L, 58.0},
                    new Object[]{1593662400000L, 2L, 60.0},
                    new Object[]{1593666000000L, 2L, 62.0},
                    new Object[]{1593666000000L, 2L, 64.0}
            );

            List<Object[]> expectedHours2 = Arrays.asList(
                    new Object[]{1527825600000L, 1L, 1.0},
                    new Object[]{1527825600000L, 1L, 2.0},
                    new Object[]{1527825600000L, 1L, 3.0},
                    new Object[]{1527825600000L, 1L, 4.0},
                    new Object[]{1527829200000L, 1L, 5.0},
                    new Object[]{1527829200000L, 1L, 6.0},
                    new Object[]{1527829200000L, 1L, 7.0},
                    new Object[]{1527829200000L, 1L, 8.0},
                    new Object[]{1527912000000L, 1L, 9.0},
                    new Object[]{1527912000000L, 1L, 10.0},
                    new Object[]{1527912000000L, 1L, 11.0},
                    new Object[]{1527912000000L, 1L, 12.0},
                    new Object[]{1527915600000L, 1L, 13.0},
                    new Object[]{1527915600000L, 1L, 14.0},
                    new Object[]{1527915600000L, 1L, 15.0},
                    new Object[]{1527915600000L, 1L, 16.0},
                    new Object[]{1530417600000L, 1L, 17.0},
                    new Object[]{1530417600000L, 1L, 18.0},
                    new Object[]{1530417600000L, 1L, 19.0},
                    new Object[]{1530417600000L, 1L, 20.0},
                    new Object[]{1530421200000L, 1L, 21.0},
                    new Object[]{1530421200000L, 1L, 22.0},
                    new Object[]{1530421200000L, 1L, 23.0},
                    new Object[]{1530421200000L, 1L, 24.0},
                    new Object[]{1530504000000L, 1L, 25.0},
                    new Object[]{1530504000000L, 1L, 26.0},
                    new Object[]{1530504000000L, 1L, 27.0},
                    new Object[]{1530504000000L, 1L, 28.0},
                    new Object[]{1530507600000L, 1L, 29.0},
                    new Object[]{1530507600000L, 1L, 30.0},
                    new Object[]{1530507600000L, 1L, 31.0},
                    new Object[]{1530507600000L, 1L, 32.0},
                    new Object[]{1590984000000L, 1L, 33.0},
                    new Object[]{1590984000000L, 1L, 34.0},
                    new Object[]{1590984000000L, 1L, 35.0},
                    new Object[]{1590984000000L, 1L, 36.0},
                    new Object[]{1590987600000L, 1L, 37.0},
                    new Object[]{1590987600000L, 1L, 38.0},
                    new Object[]{1590987600000L, 1L, 39.0},
                    new Object[]{1590987600000L, 1L, 40.0},
                    new Object[]{1591070400000L, 1L, 41.0},
                    new Object[]{1591070400000L, 1L, 42.0},
                    new Object[]{1591070400000L, 1L, 43.0},
                    new Object[]{1591070400000L, 1L, 44.0},
                    new Object[]{1591074000000L, 1L, 45.0},
                    new Object[]{1591074000000L, 1L, 46.0},
                    new Object[]{1591074000000L, 1L, 47.0},
                    new Object[]{1591074000000L, 1L, 48.0},
                    new Object[]{1593576000000L, 1L, 49.0},
                    new Object[]{1593576000000L, 1L, 50.0},
                    new Object[]{1593576000000L, 1L, 51.0},
                    new Object[]{1593576000000L, 1L, 52.0},
                    new Object[]{1593579600000L, 1L, 53.0},
                    new Object[]{1593579600000L, 1L, 54.0},
                    new Object[]{1593579600000L, 1L, 55.0},
                    new Object[]{1593579600000L, 1L, 56.0},
                    new Object[]{1593662400000L, 1L, 57.0},
                    new Object[]{1593662400000L, 1L, 58.0},
                    new Object[]{1593662400000L, 1L, 59.0},
                    new Object[]{1593662400000L, 1L, 60.0},
                    new Object[]{1593666000000L, 1L, 61.0},
                    new Object[]{1593666000000L, 1L, 62.0},
                    new Object[]{1593666000000L, 1L, 63.0},
                    new Object[]{1593666000000L, 1L, 64.0}
            );

            List<Object[]> expectedDays = Arrays.asList(
                    new Object[]{1527811200000L, 4L, 6.0},
                    new Object[]{1527811200000L, 4L, 8.0},
                    new Object[]{1527897600000L, 4L, 14.0},
                    new Object[]{1527897600000L, 4L, 16.0},
                    new Object[]{1530403200000L, 4L, 22.0},
                    new Object[]{1530403200000L, 4L, 24.0},
                    new Object[]{1530489600000L, 4L, 30.0},
                    new Object[]{1530489600000L, 4L, 32.0},
                    new Object[]{1590969600000L, 4L, 38.0},
                    new Object[]{1590969600000L, 4L, 40.0},
                    new Object[]{1591056000000L, 4L, 46.0},
                    new Object[]{1591056000000L, 4L, 48.0},
                    new Object[]{1593561600000L, 4L, 54.0},
                    new Object[]{1593561600000L, 4L, 56.0},
                    new Object[]{1593648000000L, 4L, 62.0},
                    new Object[]{1593648000000L, 4L, 64.0}
            );

            List<Object[]> expectedDays2 = Arrays.asList(
                    new Object[]{1590969600000L, 2L, 40.0},
                    new Object[]{1530403200000L, 2L, 24.0},
                    new Object[]{1530403200000L, 2L, 21.0},
                    new Object[]{1530489600000L, 2L, 31.0},
                    new Object[]{1527811200000L, 2L, 6.0},
                    new Object[]{1593648000000L, 2L, 63.0},
                    new Object[]{1593561600000L, 2L, 54.0},
                    new Object[]{1591056000000L, 2L, 46.0},
                    new Object[]{1527897600000L, 2L, 15.0},
                    new Object[]{1590969600000L, 2L, 37.0},
                    new Object[]{1593561600000L, 2L, 56.0},
                    new Object[]{1527811200000L, 2L, 8.0},
                    new Object[]{1530489600000L, 2L, 30.0},
                    new Object[]{1593561600000L, 2L, 53.0},
                    new Object[]{1527811200000L, 2L, 5.0},
                    new Object[]{1530403200000L, 2L, 22.0},
                    new Object[]{1591056000000L, 2L, 47.0},
                    new Object[]{1590969600000L, 2L, 38.0},
                    new Object[]{1530489600000L, 2L, 29.0},
                    new Object[]{1527811200000L, 2L, 7.0},
                    new Object[]{1593648000000L, 2L, 64.0},
                    new Object[]{1591056000000L, 2L, 48.0},
                    new Object[]{1593561600000L, 2L, 55.0},
                    new Object[]{1593648000000L, 2L, 61.0},
                    new Object[]{1527897600000L, 2L, 14.0},
                    new Object[]{1590969600000L, 2L, 39.0},
                    new Object[]{1527897600000L, 2L, 13.0},
                    new Object[]{1593648000000L, 2L, 62.0},
                    new Object[]{1591056000000L, 2L, 45.0},
                    new Object[]{1530403200000L, 2L, 23.0},
                    new Object[]{1530489600000L, 2L, 32.0},
                    new Object[]{1527897600000L, 2L, 16.0}
            );

            List<Object[]> expectedMonths = Arrays.asList(
                    new Object[]{1527811200000L, 8L, 14.0},
                    new Object[]{1527811200000L, 8L, 16.0},
                    new Object[]{1530403200000L, 8L, 30.0},
                    new Object[]{1530403200000L, 8L, 32.0},
                    new Object[]{1590969600000L, 8L, 46.0},
                    new Object[]{1590969600000L, 8L, 48.0},
                    new Object[]{1593561600000L, 8L, 62.0},
                    new Object[]{1593561600000L, 8L, 64.0}
            );

            List<Object[]> expectedMonths2 = Arrays.asList(
                    new Object[]{1590969600000L, 4L, 48.0},
                    new Object[]{1527811200000L, 4L, 15.0},
                    new Object[]{1530403200000L, 4L, 32.0},
                    new Object[]{1593561600000L, 4L, 63.0},
                    new Object[]{1530403200000L, 4L, 29.0},
                    new Object[]{1527811200000L, 4L, 14.0},
                    new Object[]{1593561600000L, 4L, 62.0},
                    new Object[]{1590969600000L, 4L, 45.0},
                    new Object[]{1590969600000L, 4L, 47.0},
                    new Object[]{1593561600000L, 4L, 64.0},
                    new Object[]{1527811200000L, 4L, 16.0},
                    new Object[]{1593561600000L, 4L, 61.0},
                    new Object[]{1527811200000L, 4L, 13.0},
                    new Object[]{1530403200000L, 4L, 30.0},
                    new Object[]{1530403200000L, 4L, 31.0},
                    new Object[]{1590969600000L, 4L, 46.0}
            );

            List<Object[]> expectedYears = Arrays.asList(
                    new Object[]{1514764800000L, 16L, 30.0},
                    new Object[]{1514764800000L, 16L, 32.0},
                    new Object[]{1577836800000L, 16L, 62.0},
                    new Object[]{1577836800000L, 16L, 64.0}
            );

            List<Object[]> expectedYears2 = Arrays.asList(
                    new Object[]{1514764800000L, 8L, 30.0},
                    new Object[]{1577836800000L, 8L, 61.0},
                    new Object[]{1577836800000L, 8L, 62.0},
                    new Object[]{1514764800000L, 8L, 32.0},
                    new Object[]{1514764800000L, 8L, 31.0},
                    new Object[]{1577836800000L, 8L, 63.0},
                    new Object[]{1514764800000L, 8L, 29.0},
                    new Object[]{1577836800000L, 8L, 64.0}
            );


            // Wait for events
            SiddhiTestHelper.waitForEvents(100, 32, assertMinutesInfo.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertMinutes2Info.eventCount, 60000);
            log.info("Wait over for: minutes - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 32, assertHoursInfo.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 64, assertHours2Info.eventCount, 60000);
            log.info("Wait over for: hours - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 16, assertDaysInfo.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 32, assertDays2Info.eventCount, 60000);
            log.info("Wait over for: days - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 8, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 16, assertMonthsInfo.eventCount, 60000);
            log.info("Wait over for: months - group by AGG_TIMESTAMP, context, version");
            SiddhiTestHelper.waitForEvents(100, 4, assertYearsInfo.eventCount, 60000);
            log.info("Wait over for: years - group by AGG_TIMESTAMP, context");
            SiddhiTestHelper.waitForEvents(100, 8, assertYearsInfo.eventCount, 60000);
            log.info("Wait over for: years - group by AGG_TIMESTAMP, context, version");

            // Assert with events got through Siddhi join
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutesInfo.events, expectedMinutes), "minutes - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMinutes2Info.events, expectedMinutes2), "minutes - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHoursInfo.events, expectedHours), "hours - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertHours2Info.events, expectedHours2), "hours - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDaysInfo.events, expectedDays), "days - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertDays2Info.events, expectedDays2), "days - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonthsInfo.events, expectedMonths), "months - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertMonths2Info.events, expectedMonths2), "months - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertYearsInfo.events, expectedYears), "years - group by AGG_TIMESTAMP, context matched via Siddhi runtime");
            Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(assertYears2Info.events, expectedYears2), "years - group by AGG_TIMESTAMP, context, version matched via Siddhi runtime");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

}
