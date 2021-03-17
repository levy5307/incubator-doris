// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.plugin.audit;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.producer.SimpleProducer;
import com.xiaomi.infra.galaxy.talos.producer.TalosProducerConfig;
import com.xiaomi.infra.galaxy.talos.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.Message;
import libthrift091.TException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class DorisTalosLoader {

    private final static Logger LOG = LogManager.getLogger(DorisTalosLoader.class);

    private final static String GALAXY_TALOS_SERVICE_ENDPOINT = "galaxy.talos.service.endpoint";
    private final static int DEFAULT_PARTITION_ID = 0;
    private final static int TRY_WRITE_BUFFER_TIME_LIMIT = 3;

    private SimpleProducer simpleProducer = null;
    private String topicName = "";

    public DorisTalosLoader(LoadAuditTalosLoaderPlugin.AuditLoaderConf conf) throws TException {
        Properties talosProperties = new Properties();
        talosProperties.setProperty(GALAXY_TALOS_SERVICE_ENDPOINT, conf.talosServiceEndpoint);
        String appKeyId = "Service-Admin#" + conf.appKey + "#" + conf.organizationId;
        String appSecret = conf.appSecret;
        Credential credential = new Credential().setSecretKeyId(appKeyId).setSecretKey(appSecret).
                setType(UserType.APP_SECRET);
        TalosProducerConfig producerConfig = new TalosProducerConfig(talosProperties);
        topicName = conf.topicName;
        simpleProducer = new SimpleProducer(producerConfig, topicName, DEFAULT_PARTITION_ID, credential);
    }

    public void loadBatch(List<Message> messageList) throws IOException, TException, InterruptedException {
        boolean isSucceed = false;
        int tryTimes = 0;
        while (!isSucceed) {
            try {
                tryTimes = tryTimes + 1;
                simpleProducer.putMessageList(messageList);
                isSucceed = true;
            } catch (GalaxyTalosException e) {
                if (tryTimes >= TRY_WRITE_BUFFER_TIME_LIMIT) {
                    LOG.warn("put audit message to talos total retry times: " + tryTimes);
                    throw e;
                } else {
                    if (e.getErrorCode().equals(ErrorCode.TOPIC_NOT_EXIST)) {
                        LOG.warn(topicName + "  :  " + e.getErrMsg());
                        throw e;
                    }
                    Thread.sleep(500 * tryTimes);
                    LOG.warn("talos producer put messageList, retry time: " + tryTimes +
                            ", error detail : " + e.details);
                }
            }
        }

    }

    public void close() {
        simpleProducer.shutDown();
    }
}
