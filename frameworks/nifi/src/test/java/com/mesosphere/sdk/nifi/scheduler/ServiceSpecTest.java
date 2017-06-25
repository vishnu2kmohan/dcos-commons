package com.mesosphere.sdk.nifi.scheduler;

import com.mesosphere.sdk.testing.BaseServiceSpecTest;
import org.junit.Test;

public class ServiceSpecTest extends BaseServiceSpecTest {

    public ServiceSpecTest() {
        super("FRAMEWORK_NAME", "nifi",
              "FRAMEWORK_PRINCIPAL", "nifi-principal",
              "FRAMEWORK_USER", "nobody",
              "FRAMEWORK_ZOOEKEEPER", "zk-1.zk:2181,zk-2.zk:2181,zk-3.zk:2181,zk-4.zk:2181,zk-5.zk:2181",

              "DEPLOY_STRATEGY", "parallel",
              "UPDATE_STRATEGY", "serial",
              "REPLACE_STRATEGY", "serial",

              "JAVA_URI", "",
              "LIBMESOS_URI", "",
              "BOOTSTRAP_URI", "",
              "EXECUTOR_URI", "",
              "NIFI_URI", "",

              "CONFIG_TEMPLATE_PATH", "nifi-scheduler",

              "NODE_COUNT", "3",
              "NODE_CPUS", "1",
              "NODE_MEM", "1024",
              "NODE_PORT", "0",

              "NODE_VIP_PREFIX", "nifi",
              "NODE_VIP_PORT", "8080",

              "NODE_RLIMIT_NOFILE_SOFT", "50000",
              "NODE_RLIMIT_NOFILE_HARD", "50000",
              "NODE_RLIMIT_NPROC_SOFT", "10000",
              "NODE_RLIMIT_NPROC_HARD", "10000",

              "NODE_PLACEMENT", "hostname:UNIQUE",

              "NODE_CONTENT_REPOSITORY_DISK_PATH", "content_repository",
              "NODE_CONTENT_REPOSITORY_DISK_TYPE", "ROOT",
              "NODE_CONTENT_REPOSITORY_DISK_SIZE", "1000",

              "NODE_DATABASE_REPOSITORY_DISK_PATH", "database_repository",
              "NODE_DATABASE_REPOSITORY_DISK_TYPE", "ROOT",
              "NODE_DATABASE_REPOSITORY_DISK_SIZE", "1000",

              "NODE_FLOWFILE_REPOSITORY_DISK_PATH", "flowfile_repository",
              "NODE_FLOWFILE_REPOSITORY_DISK_TYPE", "ROOT",
              "NODE_FLOWFILE_REPOSITORY_DISK_SIZE", "1000",

              "NODE_PROVENANCE_REPOSITORY_DISK_PATH", "provenance_repository",
              "NODE_PROVENANCE_REPOSITORY_DISK_TYPE", "ROOT",
              "NODE_PROVENANCE_REPOSITORY_DISK_SIZE", "1000",

              "NIFI_VERSION", "1.3.0",

              "NIFI_LDAP_AUTHENTICATION_STRATEGY", "START_TLS",
              "NIFI_LDAP_MANAGER_DN", "user@example.com",
              "NIFI_LDAP_MANAGER_PASSWORD", "password",
              "NIFI_LDAP_TLS_KEYSTORE_PATH", "./conf/keystore.jks",
              "NIFI_LDAP_TLS_KEYSTORE_PASSWORD", "keystore-password-changeit",
              "NIFI_LDAP_TLS_KEYSTORE_TYPE", "PKCS12",
              "NIFI_LDAP_TLS_TRUSTSTORE_PATH", "./conf/truststore.jks",
              "NIFI_LDAP_TLS_TRUSTSTORE_PASSWORD", "truststore-password-changeit",
              "NIFI_LDAP_TLS_TRUSTSTORE_TYPE", "PKCS12",
              "NIFI_LDAP_TLS_CLIENT_AUTH", "REQUIRED",
              "NIFI_LDAP_TLS_PROTOCOL", "TLSv1.2",
              "NIFI_LDAP_TLS_SHUTDOWN_GRACEFULLY", "false",
              "NIFI_LDAP_REFERRAL_STRATEGY", "FOLLOW",
              "NIFI_LDAP_CONNECT_TIMEOUT", "10 secs",
              "NIFI_LDAP_READ_TIMEOUT", "10 secs",
              "NIFI_LDAP_URL", "ldap://<hostname>:<port>",
              "NIFI_LDAP_USER_SEARCH_BASE", "CN=Users,DC=example,DC=com",
              "NIFI_LDAP_USER_SEARCH_FILTER", "sAMAccountName={0}",
              "NIFI_LDAP_IDENTITY_STRATEGY", "USE_DN",
              "NIFI_LDAP_AUTHENTICATION_EXPIRATION", "12 hours",

              "NIFI_FLOW_CONFIGURATION_FILE", "./conf/flow.xml.gz",
              "NIFI_FLOW_CONFIGURATION_ARCHIVE_ENABLED", "true",
              "NIFI_FLOW_CONFIGURATION_ARCHIVE_DIR", "./conf/archive/",
              "NIFI_FLOW_CONFIGURATION_ARCHIVE_MAX_TIME", "30 days",
              "NIFI_FLOW_CONFIGURATION_ARCHIVE_MAX_STORAGE", "500 MB",
              "NIFI_FLOW_CONFIGURATION_ARCHIVE_MAX_COUNT", "",
              "NIFI_FLOWCONTROLLER_AUTORESUMESTATE", "true",
              "NIFI_FLOWCONTROLLER_GRACEFUL_SHUTDOWN_PERIOD", "10 secs",
              "NIFI_FLOWSERVICE_WRITEDELAY_INTERVAL", "500 millis",

              "NIFI_ADMINISTRATIVE_YIELD_DURATION", "30 secs",
              "NIFI_BORED_YIELD_DURATION", "10 millis",

              "NIFI_AUTHORIZER_CONFIGURATION_FILE", "./conf/authorizers.xml",
              "NIFI_LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE", "./conf/login-identity-providers.xml",

              "NIFI_TEMPLATES_DIRECTORY", "./conf/templates",
              "NIFI_UI_BANNER_TEXT", "",
              "NIFI_UI_AUTOREFRESH_INTERVAL", "30 secs",
              "NIFI_NAR_LIBRARY_DIRECTORY", "./lib",
              "NIFI_NAR_WORKING_DIRECTORY", "./work/nar/",
              "NIFI_DOCUMENTATION_WORKING_DIRECTORY", "./work/docs/components",
              "NIFI_PROCESSOR_SCHEDULING_TIMEOUT", "1 mins",

              "NIFI_STATE_MANAGEMENT_CONFIGURATION_FILE", "./conf/state-management.xml",
              "NIFI_STATE_MANAGEMENT_PROVIDER_LOCAL", "local-provider",
              "NIFI_STATE_MANAGEMENT_PROVIDER_CLUSTER", "zk-provider",
              "NIFI_STATE_MANAGEMENT_EMBEDDED_ZOOKEEPER_START", "false",
              "NIFI_STATE_MANAGEMENT_EMBEDDED_ZOOKEEPER_PROPERTIES", "./conf/zookeeper.properties",

              "NIFI_DATABASE_DIRECTORY", "./database_repository",
              "NIFI_H2_URL_APPEND", ";LOCK_TIMEOUT=25000;WRITE_DELAY=0;AUTO_SERVER=FALSE",

              "NIFI_FLOWFILE_REPOSITORY_IMPLEMENTATION", "org.apache.nifi.controller.repository.WriteAheadFlowFileRepository",
              "NIFI_FLOWFILE_REPOSITORY_DIRECTORY", "./flowfile_repository",
              "NIFI_FLOWFILE_REPOSITORY_PARTITIONS", "256",
              "NIFI_FLOWFILE_REPOSITORY_CHECKPOINT_INTERVAL", "2 mins",
              "NIFI_FLOWFILE_REPOSITORY_ALWAYS_SYNC", "false",

              "NIFI_SWAP_MANAGER_IMPLEMENTATION", "org.apache.nifi.controller.FileSystemSwapManager",
              "NIFI_QUEUE_SWAP_THRESHOLD", "20000",
              "NIFI_SWAP_IN_PERIOD", "5 secs",
              "NIFI_SWAP_IN_THREADS", "1",
              "NIFI_SWAP_OUT_PERIOD", "5 secs",
              "NIFI_SWAP_OUT_THREADS", "4",

              "NIFI_CONTENT_REPOSITORY_IMPLEMENTATION", "org.apache.nifi.controller.repository.FileSystemRepository",
              "NIFI_CONTENT_CLAIM_MAX_APPENDABLE_SIZE", "10 MB",
              "NIFI_CONTENT_CLAIM_MAX_FLOW_FILES", "100",
              "NIFI_CONTENT_REPOSITORY_DIRECTORY_DEFAULT", "./content_repository",
              "NIFI_CONTENT_REPOSITORY_ARCHIVE_MAX_RETENTION_PERIOD", "12 hours",
              "NIFI_CONTENT_REPOSITORY_ARCHIVE_MAX_USAGE_PERCENTAGE", "50%",
              "NIFI_CONTENT_REPOSITORY_ARCHIVE_ENABLED", "true",
              "NIFI_CONTENT_REPOSITORY_ALWAYS_SYNC", "false",
              "NIFI_CONTENT_VIEWER_URL", "/nifi-content-viewer/",

              "NIFI_VOLATILE_CONTENT_REPOSITORY_MAX_SIZE", "100 MB",
              "NIFI_VOLATILE_CONTENT_REPOSITORY_BLOCK_SIZE", "32 KB",

              "NIFI_PROVENANCE_REPOSITORY_IMPLEMENTATION", "org.apache.nifi.provenance.PersistentProvenanceRepository",

              "NIFI_PROVENANCE_REPOSITORY_DIRECTORY_DEFAULT", "./provenance_repository",
              "NIFI_PROVENANCE_REPOSITORY_MAX_STORAGE_TIME", "24 hours",
              "NIFI_PROVENANCE_REPOSITORY_MAX_STORAGE_SIZE", "1 GB",
              "NIFI_PROVENANCE_REPOSITORY_ROLLOVER_TIME", "30 secs",
              "NIFI_PROVENANCE_REPOSITORY_ROLLOVER_SIZE", "100 MB",
              "NIFI_PROVENANCE_REPOSITORY_QUERY_THREADS", "2",
              "NIFI_PROVENANCE_REPOSITORY_INDEX_THREADS", "2",
              "NIFI_PROVENANCE_REPOSITORY_COMPRESS_ON_ROLLOVER", "true",
              "NIFI_PROVENANCE_REPOSITORY_ALWAYS_SYNC", "false",
              "NIFI_PROVENANCE_REPOSITORY_JOURNAL_COUNT", "16",
              "NIFI_PROVENANCE_REPOSITORY_INDEXED_FIELDS", "EventType, FlowFileUUID, Filename, ProcessorID, Relationship",
              "NIFI_PROVENANCE_REPOSITORY_INDEXED_ATTRIBUTES", "",
              "NIFI_PROVENANCE_REPOSITORY_INDEX_SHARD_SIZE", "500 MB",
              "NIFI_PROVENANCE_REPOSITORY_MAX_ATTRIBUTE_LENGTH", "65536",

              "NIFI_PROVENANCE_REPOSITORY_BUFFER_SIZE", "100000",

              "NIFI_PROVENANCE_REPOSITORY_CONCURRENT_MERGE_THREADS", "2",
              "NIFI_PROVENANCE_REPOSITORY_WARM_CACHE_FREQUENCY", "",

              "NIFI_PROVENANCE_REPOSITORY_DEBUG_FREQUENCY", "1_000_000",

              "NIFI_PROVENANCE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION", "",
              "NIFI_PROVENANCE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION", "",
              "NIFI_PROVENANCE_REPOSITORY_ENCRYPTION_KEY_ID", "",
              "NIFI_PROVENANCE_REPOSITORY_ENCRYPTION_KEY", "",

              "NIFI_COMPONENTS_STATUS_REPOSITORY_IMPLEMENTATION", "org.apache.nifi.controller.status.history.VolatileComponentStatusRepository",
              "NIFI_COMPONENTS_STATUS_REPOSITORY_BUFFER_SIZE", "1440",
              "NIFI_COMPONENTS_STATUS_SNAPSHOT_FREQUENCY", "1 mins",

              "NIFI_REMOTE_INPUT_HOST", "",
              "NIFI_REMOTE_INPUT_SECURE", "false",
              "NIFI_REMOTE_INPUT_SOCKET_PORT", "",
              "NIFI_REMOTE_INPUT_HTTP_ENABLED", "true",
              "NIFI_REMOTE_INPUT_HTTP_TRANSACTION_TTL", "30 secs",

              "NIFI_WEB_WAR_DIRECTORY", "./lib",
              "NIFI_WEB_HTTP_HOST", "",
              "NIFI_WEB_HTTP_PORT", "8080",
              "NIFI_WEB_HTTP_NETWORK_INTERFACE_DEFAULT", "",
              "NIFI_WEB_HTTPS_HOST", "",
              "NIFI_WEB_HTTPS_PORT", "8443",
              "NIFI_WEB_HTTPS_NETWORK_INTERFACE_DEFAULT", "",
              "NIFI_WEB_JETTY_WORKING_DIRECTORY", "./work/jetty",
              "NIFI_WEB_JETTY_THREADS", "200",

              "NIFI_SENSITIVE_PROPS_KEY", "",
              "NIFI_SENSITIVE_PROPS_KEY_PROTECTED", "",
              "NIFI_SENSITIVE_PROPS_ALGORITHM", "PBEWITHMD5AND256BITAES-CBC-OPENSSL",
              "NIFI_SENSITIVE_PROPS_PROVIDER", "BC",
              "NIFI_SENSITIVE_PROPS_ADDITIONAL_KEYS", "",

              "NIFI_SECURITY_KEYSTORE", "",
              "NIFI_SECURITY_KEYSTORETYPE", "",
              "NIFI_SECURITY_KEYSTOREPASSWD", "",
              "NIFI_SECURITY_KEYPASSWD", "",

              "NIFI_SECURITY_TRUSTSTORE", "",
              "NIFI_SECURITY_TRUSTSTORETYPE", "",
              "NIFI_SECURITY_TRUSTSTOREPASSWD", "",

              "NIFI_SECURITY_NEEDCLIENTAUTH", "",
              "NIFI_SECURITY_USER_AUTHORIZER", "",
              "NIFI_SECURITY_USER_LOGIN_IDENTITY_PROVIDER", "file-provider",
              "NIFI_SECURITY_OCSP_RESPONDER_URL", "",
              "NIFI_SECURITY_OCSP_RESPONDER_CERTIFICATE", "",

              "NIFI_SECURITY_IDENTITY_MAPPING_PATTERN_DN", "^CN=(.*?), OU=(.*?), O=(.*?), L=(.*?), ST=(.*?), C=(.*?)$",
              "NIFI_SECURITY_IDENTITY_MAPPING_VALUE_DN", "$1@$2",
              "NIFI_SECURITY_IDENTITY_MAPPING_PATTERN_KERB", "^(.*?)/instance@(.*?)$",
              "NIFI_SECURITY_IDENTITY_MAPPING_VALUE_KERB", "$1@$2",

              "NIFI_CLUSTER_PROTOCOL_HEARTBEAT_INTERVAL", "5 secs",
              "NIFI_CLUSTER_PROTOCOL_IS_SECURE", "false",

              "NIFI_CLUSTER_IS_NODE", "true",
              "NIFI_CLUSTER_NODE_ADDRESS", "",
              "NIFI_CLUSTER_NODE_PROTOCOL_PORT", "0",
              "NIFI_CLUSTER_NODE_PROTOCOL_THREADS", "10",
              "NIFI_CLUSTER_NODE_EVENT_HISTORY_SIZE", "25",
              "NIFI_CLUSTER_NODE_CONNECTION_TIMEOUT", "5 secs",
              "NIFI_CLUSTER_NODE_READ_TIMEOUT", "5 secs",
              "NIFI_CLUSTER_FIREWALL_FILE", "",
              "NIFI_CLUSTER_FLOW_ELECTION_MAX_WAIT_TIME", "5 mins",
              "NIFI_CLUSTER_FLOW_ELECTION_MAX_CANDIDATES", "",

              "NIFI_ZOOKEEPER_CONNECT_STRING", "",
              "NIFI_ZOOKEEPER_CONNECT_TIMEOUT", "3 secs",
              "NIFI_ZOOKEEPER_SESSION_TIMEOUT", "3 secs",
              "NIFI_ZOOKEEPER_ROOT_NODE", "/nifi",

              "NIFI_KERBEROS_DEFAULT_REALM", "NIFI.APACHE.ORG",
              "NIFI_KERBEROS_AUTHENTICATION_EXPIRATION", "12 hours",
              "NIFI_KERBEROS_KRB5_FILE", "/etc/krb5.conf",
              "NIFI_KERBEROS_SERVICE_PRINCIPAL", "",
              "NIFI_KERBEROS_SERVICE_KEYTAB_LOCATION", "",
              "NIFI_KERBEROS_SPNEGO_PRINCIPAL", "",
              "NIFI_KERBEROS_SPNEGO_KEYTAB_LOCATION", "",
              "NIFI_KERBEROS_SPNEGO_AUTHENTICATION_EXPIRATION", "12 hours",

              "NIFI_VARIABLE_REGISTRY_PROPERTIES", "");
    }

    @Test
    public void testYmlBase() throws Exception {
        testYaml("svc.yml");
    }
}
