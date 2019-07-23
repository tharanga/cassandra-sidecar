/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar;

import javax.annotation.Nullable;

/**
 * Sidecar configuration
 */
public class Configuration
{
    /* Cassandra Host */
    private final String cassandraHost;

    /* Cassandra Port */
    private final Integer cassandraPort;

    /* Sidecar's HTTP REST API port */
    private final Integer port;

    /* Sidecar's listen address */
    private String host;

    /* Healthcheck frequency in miilis */
    private final Integer healthCheckFrequencyMillis;

    /* SSL related settings */
    @Nullable
    private final String keyStorePath;
    @Nullable
    private final String keyStorePassword;

    @Nullable
    private final String trustStorePath;
    @Nullable
    private final String trustStorePassword;

    private final boolean isSslEnabled;


    /* Cassandra conf path*/
    @Nullable
    private String cassandraConfigPath;

    @Nullable
    String keySpace;

    @Nullable
    String columnFamily;

    @Nullable
    String kafkaServer;

    @Nullable
    String kafkaTopic;

    public Configuration(String cassandraHost, Integer cassandraPort, String host, Integer port,
                         Integer healthCheckFrequencyMillis, boolean isSslEnabled,
                         @Nullable String keyStorePath,
                         @Nullable String keyStorePassword,
                         @Nullable String trustStorePath,
                         @Nullable String trustStorePassword,
                         @Nullable String cassandraConfigPath,
                         @Nullable String keySpace,
                         @Nullable String columnFamily,
                         @Nullable String kafkaServer,
                         @Nullable String kafkaTopic)
    {
        this.cassandraHost = cassandraHost;
        this.cassandraPort = cassandraPort;
        this.host = host;
        this.port = port;
        this.healthCheckFrequencyMillis = healthCheckFrequencyMillis;

        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        this.isSslEnabled = isSslEnabled;


        this.cassandraConfigPath = cassandraConfigPath;
        this.keySpace = keySpace;
        this.columnFamily = columnFamily;
        this.kafkaServer = kafkaServer;
        this.kafkaTopic = kafkaTopic;
    }

    /**
     * Get the Cassandra host
     *
     * @return
     */
    public String getCassandraHost()
    {
        return cassandraHost;
    }

    /**
     * Get the Cassandra port
     *
     * @return
     */
    public Integer getCassandraPort()
    {
        return cassandraPort;
    }

    /**
     *  Sidecar's listen address
     *
     * @return
     */
    public String getHost()
    {
        return host;
    }

    /**
     * Get the Sidecar's REST HTTP API port
     *
     * @return
     */
    public Integer getPort()
    {
        return port;
    }

    /**
     * Get the health check frequency in millis
     *
     * @return
     */
    public Integer getHealthCheckFrequencyMillis()
    {
        return healthCheckFrequencyMillis;
    }

    /**
     * Get the SSL status
     *
     * @return
     */
    public boolean isSslEnabled()
    {
        return isSslEnabled;
    }

    /**
     * Get the Keystore Path
     *
     * @return
     */
    @Nullable
    public String getKeyStorePath()
    {
        return keyStorePath;
    }

    /**
     * Get the Keystore password
     *
     * @return
     */
    @Nullable
    public String getKeystorePassword()
    {
        return keyStorePassword;
    }

    /**
     * Get the Truststore Path
     *
     * @return
     */
    @Nullable
    public String getTrustStorePath()
    {
        return trustStorePath;
    }

    /**
     * Get the Truststore password
     *
     * @return
     */
    @Nullable
    public String getTruststorePassword()
    {
        return trustStorePassword;
    }

    /**
     * Get path of the Cassandra configuration file
     */
    @Nullable
    public String getCassandraConfigPath()
    {
        return cassandraConfigPath;
    }

    /**
     * Get keyspace of the column family
     */
    @Nullable
    public String getKeySpace()
    {
        return keySpace;
    }

    /**
     * Get the column family to extract change data/ commit logs.
     */
    @Nullable
    public String getColumnFamily()
    {
        return columnFamily;
    }

    /**
     * Get Kafka server to publish change events
     */
    @Nullable
    public String getKafkaServer()
    {
        return kafkaServer;
    }

    /**
     * Get Kafka topic to publish changes
     */
    @Nullable
    public String getKafkaTopic()
    {
        return kafkaTopic;
    }

    /**
     * Configuration Builder
     */
    public static class Builder
    {
        private String cassandraHost;
        private Integer cassandraPort;
        private String host;
        private Integer port;
        private Integer healthCheckFrequencyMillis;
        private String keyStorePath;
        private String keyStorePassword;
        private String trustStorePath;
        private String trustStorePassword;
        private boolean isSslEnabled;
        private String cassandraConfigPath;
        private String cdcKeySpace;
        private String cdcColumnFamily;
        private String cdcKafkaServer;
        private String cdcKafkaTopic;

        public Builder setCassandraHost(String host)
        {
            this.cassandraHost = host;
            return this;
        }

        public Builder setCassandraPort(Integer port)
        {
            this.cassandraPort = port;
            return this;
        }

        public Builder setHost(String host)
        {
            this.host = host;
            return this;
        }

        public Builder setPort(Integer port)
        {
            this.port = port;
            return this;
        }

        public Builder setHealthCheckFrequency(Integer freqMillis)
        {
            this.healthCheckFrequencyMillis = freqMillis;
            return this;
        }

        public Builder setKeyStorePath(String path)
        {
            this.keyStorePath = path;
            return this;
        }

        public Builder setKeyStorePassword(String password)
        {
            this.keyStorePassword = password;
            return this;
        }

        public Builder setTrustStorePath(String path)
        {
            this.trustStorePath = path;
            return this;
        }

        public Builder setTrustStorePassword(String password)
        {
            this.trustStorePassword = password;
            return this;
        }

        public Builder setSslEnabled(boolean enabled)
        {
            this.isSslEnabled = enabled;
            return this;
        }

        public Builder setCdcKeySpace(String cdcKeySpace)
        {
            this.cdcKeySpace = cdcKeySpace;
            return  this;
        }

        public Builder setCdcColumnFamily(String cdcColumnFamily)
        {
            this.cdcColumnFamily = cdcColumnFamily;
            return  this;
        }

        public Builder setCdcKafkaServer(String kafkaServer)
        {
            this.cdcKafkaServer = kafkaServer;
            return  this;
        }

        public Builder setCdcKafkaTopic(String kafkaTopic)
        {
            this.cdcKafkaTopic = kafkaTopic;
            return  this;
        }

        public Builder setCassandraConfigPath(String configPath)
        {
            this.cassandraConfigPath = configPath;
            return  this;
        }

        public Configuration build()
        {
            return new Configuration(cassandraHost, cassandraPort, host, port, healthCheckFrequencyMillis, isSslEnabled,
                                     keyStorePath, keyStorePassword, trustStorePath, trustStorePassword,
                    cassandraConfigPath, cdcKeySpace, cdcColumnFamily, cdcKafkaServer, cdcKafkaTopic);
        }
    }
}
