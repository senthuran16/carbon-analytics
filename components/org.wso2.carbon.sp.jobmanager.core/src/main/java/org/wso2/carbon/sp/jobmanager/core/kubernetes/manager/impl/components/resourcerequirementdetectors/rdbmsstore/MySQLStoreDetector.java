package org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.components.resourcerequirementdetectors.rdbmsstore;

import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.framework.models.concrete.ResourceRequirement;
import org.wso2.carbon.sp.jobmanager.core.kubernetes.manager.impl.components.resourcerequirementdetectors.SiddhiResourceRequirementDetector;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.TableDefinition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Detects MySQL RDBMS Store requirement from a Siddhi application
 */
public class MySQLStoreDetector extends SiddhiResourceRequirementDetector {
    // TODO user has to look at these and tag the resource deployment. This should be provided to the user
    public static final String STORE_RDBMS_MYSQL_KEY = "store-rdbms-mysql";
    public static final String STORE_RDBMS_MYSQL_VALUE = "true";

    @Override
    public ResourceRequirement generateResourceRequirement(SiddhiApp parsedApp,
                                                           SiddhiAppRuntime appRuntime,
                                                           String appString) {
        Map<String, TableDefinition> tableDefinitionMap = parsedApp.getTableDefinitionMap();
        for (Map.Entry<String, TableDefinition> tableDefinitionEntry : tableDefinitionMap.entrySet()) {
            List<Annotation> annotations = tableDefinitionEntry.getValue().getAnnotations();
            if (annotations != null) {
                if (isMySQLStore(annotations)) {
                    Map<String, String> labels = new HashMap<>();
                    labels.put(STORE_RDBMS_MYSQL_KEY, STORE_RDBMS_MYSQL_KEY);
                    return new ResourceRequirement(generateAffinityLabels());
                }
            }
        }
        return null;
    }

    @Override
    public Map<String, String> generateAffinityLabels() {
        Map<String, String> labels = new HashMap<>();
        labels.put(STORE_RDBMS_MYSQL_KEY, STORE_RDBMS_MYSQL_VALUE);
        return labels;
    }

    private boolean isMySQLStore(List<Annotation> annotations) {
        for (Annotation annotation : annotations) {
            if (annotation.getName().equalsIgnoreCase("Store")) {
                return isMySQL(annotation);
            }
        }
        return false;
    }

    private boolean isMySQL(Annotation storeAnnotation) {
        boolean isRDBMSDetected = false;
        boolean isMYSQLDriverDetected = false;
        for (Element element : storeAnnotation.getElements()) {
            if (!isRDBMSDetected) {
                if (element.getKey().equalsIgnoreCase("type") &&
                        element.getValue().equalsIgnoreCase("rdbms")) {
                    isRDBMSDetected = true;
                }
            }
            if (!isMYSQLDriverDetected) {
                if (element.getKey().equalsIgnoreCase("jdbc.driver.name") &&
                        element.getValue().equalsIgnoreCase("com.mysql.jdbc.driver")) {
                    isMYSQLDriverDetected = true;
                }
            }
        }
        return isRDBMSDetected && isMYSQLDriverDetected;
    }
}
