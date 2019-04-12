/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.cassandra.client;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.base.Objects;
import com.netflix.config.DynamicPropertyFactory;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.utils.config.ZookeeperUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.secret.management.encriptor.SecretEncriptor;
import com.orwellg.umbrella.secret.management.encriptor.SecretEncriptorFactory;
import com.orwellg.umbrella.secret.management.encriptor.SecretEncriptorFactory.SecretEncriptorTypes;

/**
 * Configuration used by cassandra storm components.
 */
public class CassandraConf implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final static Logger LOG = LoggerFactory.getLogger(CassandraConf.class);

	public static final String ZK_HOST_LIST = "zookeeper.host.list";
	
	public static final String CASSANDRA_ZOOKEEPER_PATH     = "cassandra.zookeeper.path";
    public static final String CASSANDRA_USERNAME           = "cassandra.username";
    public static final String CASSANDRA_PASSWORD           = "cassandra.password";
    public static final String CASSANDRA_KEYSPACE           = "cassandra.keyspace";
    public static final String CASSANDRA_CONSISTENCY_LEVEL  = "cassandra.output.consistencyLevel";
    public static final String CASSANDRA_NODES              = "cassandra.nodes";
    public static final String CASSANDRA_PORT               = "cassandra.port";
    public static final String CASSANDRA_BATCH_SIZE_ROWS    = "cassandra.batch.size.rows";
    public static final String CASSANDRA_RETRY_POLICY       = "cassandra.retryPolicy";
    public static final String CASSANDRA_RECONNECT_POLICY_BASE_MS  = "cassandra.reconnectionPolicy.baseDelayMs";
    public static final String CASSANDRA_RECONNECT_POLICY_MAX_MS   = "cassandra.reconnectionPolicy.maxDelayMs";
    public static final String CASSANDRA_SSL_SECURITY_PROTOCOL = "cassandra.ssl.security.protocol";
    public static final String CASSANDRA_SSL_KEYSTORE_PATH = "cassandra.ssl.keystore.path";
    public static final String CASSANDRA_SSL_KEYSTORE_PASSWORD = "cassandra.ssl.keystore.password";
    public static final String CASSANDRA_SSL_TRUSTSTORE_PATH = "cassandra.ssl.truststore.path";
    public static final String CASSANDRA_SSL_TRUSTSTORE_PASSWORD = "cassandra.ssl.truststore.password";
    
    public static final String CASSANDRA_DECRYPT_FILE_PATH = "cassandra.decrypt.file.path";

    /**
     * The authorized cassandra username.
     */
    private String username;
    /**
     * The authorized cassandra password
     */
    private String password;
    /**
     * The cassandra keyspace.
     */
    private String keyspace;
    /**
     * List of contacts nodes.
     */
    private String[] nodes = {"localhost"};

    /**
     * The port used to connect to nodes.
     */
    private int port = 9092;

    /**
     * Consistency level used to write statements.
     */
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
    /**
     * The maximal numbers of rows per batch.
     */
    private int batchSizeRows       = 100;

    /**
     * The retry policy to use for the new cluster.
     */
    private String retryPolicyName;

    /**
     * The base delay in milliseconds to use for the reconnection policy.
     */
    private long reconnectionPolicyBaseMs;

    /**
     * The maximum delay to wait between two attempts.
     */
    private long reconnectionPolicyMaxMs;

    private String sslSecurityProtocol;

    private String sslTruststorePath;

    private String sslTruststorePassword;

    private String sslKeystorePath;

    private String sslKeystorePassword;
    
    private String decryptFilePath;
    
    private SecretEncriptor<?> encryptor;

    /**
     * Creates a new {@link CassandraConf} instance.
     */
    public CassandraConf() {
        super();
    }

    /**
     * Creates a new {@link CassandraConf} instance.
     *
     * @param conf The storm configuration.
     */
    public CassandraConf(Map<String, Object> conf) {

    	
    	DynamicPropertyFactory dynamicFactory = null;
    	if (conf.containsKey(CASSANDRA_ZOOKEEPER_PATH)) {   	
    		String zookeeper = (String) Utils.get(conf, ZK_HOST_LIST, null);
    		String path = (String) Utils.get(conf, CASSANDRA_ZOOKEEPER_PATH, null);    	
    		try {
    			LOG.info("Loading the properties using the zookeeper host {} and cassandra path {}.", zookeeper, path);
    			ZookeeperUtils.init(zookeeper, Arrays.asList(new String[] {path}));
    			ZookeeperUtils.addConfigurationSource(path);
    			dynamicFactory = ZookeeperUtils.getDynamicPropertyFactory();
    		} catch (Exception e) {
    			LOG.error("Cannot load the properties using the zookeeper host {} and cassandra path {}. Message: {}", zookeeper, path, e.getMessage(), e);
    			dynamicFactory = null;
    		}
    	}	
    	
    	if (dynamicFactory != null) {
   			LOG.info("Loading scylla config from zookeeper");
            this.username = dynamicFactory.getStringProperty(ScyllaParams.USER_PROP_NAME, null).get();
	        this.password = dynamicFactory.getSecretProperty(ScyllaParams.PASSWORD_PROP_NAME, null).get();
	        String lst = dynamicFactory.getStringProperty(Constants.SCYLLA_NODE_HOST_LIST, null).get();
	        this.nodes    = (!StringUtils.isEmpty(lst)) ? lst.split(",") : null;    		
            this.sslSecurityProtocol = dynamicFactory.getStringProperty(ScyllaParams.SSL_SECURITY_PROTOCOL_PROP_NAME, null).get();
            this.sslKeystorePath     = dynamicFactory.getStringProperty(ScyllaParams.SSL_KEYSTORE_PATH_PROP_NAME, null).get();
            this.sslKeystorePassword = dynamicFactory.getSecretProperty(ScyllaParams.SSL_KEYSTORE_PASSWORD_PROP_NAME, null).get();
            this.sslTruststorePath   = dynamicFactory.getStringProperty(ScyllaParams.SSL_TRUSTSTORE_PATH_PROP_NAME, null).get();
            this.sslTruststorePassword = dynamicFactory.getSecretProperty(ScyllaParams.SSL_TRUSTSTORE_PASSWORD_PROP_NAME, null).get();
    	} else {
   			LOG.info("Loading scylla config from configuration map");
	        this.username = (String) Utils.get(conf, CASSANDRA_USERNAME, null);
	        this.password = (String) Utils.get(conf, CASSANDRA_PASSWORD, null);
	        this.nodes    = ((String) Utils.get(conf, CASSANDRA_NODES, "localhost")).split(",");	        
	        this.sslSecurityProtocol = (String) Utils.get(conf, CASSANDRA_SSL_SECURITY_PROTOCOL, null);
	        this.sslKeystorePath = (String) Utils.get(conf, CASSANDRA_SSL_KEYSTORE_PATH, null);
	        this.sslKeystorePassword = (String) Utils.get(conf, CASSANDRA_SSL_KEYSTORE_PASSWORD, null);
	        this.sslTruststorePath = (String) Utils.get(conf, CASSANDRA_SSL_TRUSTSTORE_PATH, null);
	        this.sslTruststorePassword = (String) Utils.get(conf, CASSANDRA_SSL_TRUSTSTORE_PASSWORD, null);
	        this.decryptFilePath = (String) Utils.get(conf, CASSANDRA_DECRYPT_FILE_PATH, null);
	        if (!StringUtils.isEmpty(this.decryptFilePath)) { this.encryptor = SecretEncriptorFactory.getSecretEncriptor(SecretEncriptorTypes.RSA_SECRET_ENCRIPTOR); }
    	}
        this.keyspace = get(conf, CASSANDRA_KEYSPACE);
        this.consistencyLevel = ConsistencyLevel.valueOf((String) Utils.get(conf, CASSANDRA_CONSISTENCY_LEVEL, ConsistencyLevel.ONE.name()));
        this.batchSizeRows = Utils.getInt(conf.get(CASSANDRA_BATCH_SIZE_ROWS), 100);
        this.port = Utils.getInt(conf.get(CASSANDRA_PORT), 9042);
        this.retryPolicyName = (String) Utils.get(conf, CASSANDRA_RETRY_POLICY, DefaultRetryPolicy.class.getSimpleName());
        this.reconnectionPolicyBaseMs = getLong(conf.get(CASSANDRA_RECONNECT_POLICY_BASE_MS), 100L);
        this.reconnectionPolicyMaxMs  = getLong(conf.get(CASSANDRA_RECONNECT_POLICY_MAX_MS), TimeUnit.MINUTES.toMillis(1));
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
    	if (this.encryptor != null && !StringUtils.isEmpty(this.decryptFilePath)) { 
    		try {
    			return this.encryptor.decrypt(this.password, this.decryptFilePath); 
    		} catch (Exception e) {
    			throw new RuntimeException(e);
    		}
    	} else {
    		return password;
    	}
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String[] getNodes() {
        return nodes;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public int getBatchSizeRows() {
        return batchSizeRows;
    }

    public int getPort() {
        return this.port;
    }

    public long getReconnectionPolicyBaseMs() {
        return reconnectionPolicyBaseMs;
    }

    public long getReconnectionPolicyMaxMs() {
        return reconnectionPolicyMaxMs;
    }

    public RetryPolicy getRetryPolicy() {
        if(this.retryPolicyName.equals(DowngradingConsistencyRetryPolicy.class.getSimpleName()))
            return DowngradingConsistencyRetryPolicy.INSTANCE;
        if(this.retryPolicyName.equals(FallthroughRetryPolicy.class.getSimpleName()))
            return FallthroughRetryPolicy.INSTANCE;
        if(this.retryPolicyName.equals(DefaultRetryPolicy.class.getSimpleName()))
            return DefaultRetryPolicy.INSTANCE;
        throw new IllegalArgumentException("Unknown cassandra retry policy " + this.retryPolicyName);
    }

    public SslProps getSslProps() {
        return new SslProps(sslSecurityProtocol, sslTruststorePath, sslTruststorePassword, sslKeystorePath, sslKeystorePassword, decryptFilePath);
    }

    @SuppressWarnings("unchecked")
	private <T> T get(Map<String, Object> conf, String key) {
        Object o = conf.get(key);
        if(o == null) {
            throw new IllegalArgumentException("No '" + key + "' value found in configuration!");
        }
        return (T)o;
    }

    public static Long getLong(Object o, Long defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof Number) {
            return ((Number) o).longValue();
        } else if (o instanceof String) {
            return Long.parseLong((String) o);
        }
        throw new IllegalArgumentException("Don't know how to convert " + o + " to long");
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("username", username)
                .add("password", password)
                .add("keyspace", keyspace)
                .add("nodes", nodes)
                .add("port", port)
                .add("consistencyLevel", consistencyLevel)
                .add("batchSizeRows", batchSizeRows)
                .add("retryPolicyName", retryPolicyName)
                .add("reconnectionPolicyBaseMs", reconnectionPolicyBaseMs)
                .add("reconnectionPolicyMaxMs", reconnectionPolicyMaxMs)
                .toString();
    }
}
