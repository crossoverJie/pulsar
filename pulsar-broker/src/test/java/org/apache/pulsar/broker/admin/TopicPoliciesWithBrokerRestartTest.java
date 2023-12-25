/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.admin;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Test(groups = "broker-admin")
public class TopicPoliciesWithBrokerRestartTest extends MockedPulsarServiceBaseTest {

    @Override
    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        super.internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


    @Test
    public void testRetentionWithBrokerRestart() throws Exception {
        final int messages = 1_000;
        final int topicNum = 500;
        // (1) Init topic
        admin.namespaces().createNamespace("public/retention");
        final String topicName = "persistent://public/retention/retention_with_broker_restart";
        admin.topics().createNonPartitionedTopic(topicName);
        for (int i = 0; i < topicNum; i++) {
            final String shadowTopicNames = topicName + "_" + i;
            admin.topics().createNonPartitionedTopic(shadowTopicNames);
        }
        // (2) Set retention
        final RetentionPolicies retentionPolicies = new RetentionPolicies(20, 20);
        for (int i = 0; i < topicNum; i++) {
            final String shadowTopicNames = topicName + "_" + i;
            admin.topicPolicies().setRetention(shadowTopicNames, retentionPolicies);
        }
        admin.topicPolicies().setRetention(topicName, retentionPolicies);
        // (3) Send messages
        @Cleanup
        final Producer<byte[]> publisher = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        for (int i = 0; i < messages; i++) {
            publisher.send((i + "").getBytes(StandardCharsets.UTF_8));
        }
        // (4) Check configuration
        Awaitility.await().untilAsserted(() -> {
            final PersistentTopic persistentTopic1 = (PersistentTopic)
                    pulsar.getBrokerService().getTopic(topicName, true).join().get();
            final ManagedLedgerImpl managedLedger1 = (ManagedLedgerImpl) persistentTopic1.getManagedLedger();
            Assert.assertEquals(managedLedger1.getConfig().getRetentionSizeInMB(), 20);
            Assert.assertEquals(managedLedger1.getConfig().getRetentionTimeMillis(),
                    TimeUnit.MINUTES.toMillis(20));
        });
        // (5) Restart broker
        restartBroker();
        // (6) Check configuration again
        for (int i = 0; i < topicNum; i++) {
            final String shadowTopicNames = topicName + "_" + i;
            admin.lookups().lookupTopic(shadowTopicNames);
            final PersistentTopic persistentTopicTmp = (PersistentTopic)
                    pulsar.getBrokerService().getTopic(shadowTopicNames, true).join().get();
            final ManagedLedgerImpl managedLedgerTemp = (ManagedLedgerImpl) persistentTopicTmp.getManagedLedger();
            Assert.assertEquals(managedLedgerTemp.getConfig().getRetentionSizeInMB(), 20);
            Assert.assertEquals(managedLedgerTemp.getConfig().getRetentionTimeMillis(),
                    TimeUnit.MINUTES.toMillis(20));
        }
    }


    @Test
    public void testCreateTopicAndUpdatePolicyConcurrent() throws Exception {

        final int topicNum = 100;
        final int partition = 10;

        // (1) Init topic
        admin.namespaces().createNamespace("public/retention");
        final String topicName = "persistent://public/retention/policy_with_broker_restart_s";
        for (int i = 0; i < topicNum; i++) {
            final String shadowTopicNames = topicName + "_" + i;
            admin.topics().createPartitionedTopic(shadowTopicNames, partition);
        }

        // (2) Set Policy
        for (int i = 90; i < 100; i++) {
            final String shadowTopicNames = topicName + "_" + i;
            CompletableFuture.runAsync(() -> {
                while (true) {
                    PublishRate publishRate = new PublishRate();
                    publishRate.publishThrottlingRateInMsg = 100;
                    try {
                        admin.topicPolicies().setPublishRate(shadowTopicNames, publishRate);
                    } catch (PulsarAdminException e) {
                    }
                }
            });
        }

        for (int i = 90; i < 100; i++) {
            final String shadowTopicNames = topicName + "_" + i;
            CompletableFuture.runAsync(() -> {
                while (true) {
                    try {
                        admin.lookups().lookupPartitionedTopic(shadowTopicNames);
                    } catch (Exception e) {
                    }
                }
            });
        }

        admin.namespaces().unload("public/retention");
        admin.namespaces().unload("public/retention");
        admin.namespaces().unload("public/retention");
        Thread.sleep(1000* 5);

        for (int i = 0; i < topicNum; i++) {
            final String shadowTopicNames = topicName + "_" + i;
            log.info("check topic: {}", shadowTopicNames);
            PartitionedTopicStats partitionedStats = admin.topics().getPartitionedStats(shadowTopicNames, true);
            Assert.assertEquals(partitionedStats.getPartitions().size(), partition);
        }

    }
}
