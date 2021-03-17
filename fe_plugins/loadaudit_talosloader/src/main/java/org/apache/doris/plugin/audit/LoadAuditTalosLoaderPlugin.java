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

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.xiaomi.infra.galaxy.talos.thrift.Message;

import com.xiaomi.infra.galaxy.talos.thrift.MessageType;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.LoadAuditEvent;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginContext;
import org.apache.doris.plugin.PluginException;
import org.apache.doris.plugin.PluginInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 * This plugin will load audit log to specified talos topic at specified interval
 */
public class LoadAuditTalosLoaderPlugin extends Plugin implements AuditPlugin {
    private final static Logger LOG = LogManager.getLogger(LoadAuditTalosLoaderPlugin.class);

    private static SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private List<Message> auditBuffer = new ArrayList<>();
    private long auditBufferSize = 0;
    private long lastLoadTime = 0;

    private BlockingQueue<LoadAuditEvent> batchQueue = new LinkedBlockingDeque<LoadAuditEvent>();
    private DorisTalosLoader talosLoader;
    private Thread loadThread;

    private AuditLoaderConf conf;
    private volatile boolean isClosed = false;
    private volatile boolean isInit = false;

    @Override
    public void init(PluginInfo info, PluginContext ctx) throws PluginException {
        super.init(info, ctx);
        try {
            synchronized (this) {
                if (isInit) {
                    return;
                }
                this.lastLoadTime = System.currentTimeMillis();

                loadConfig(ctx, info.getProperties());

                this.talosLoader = new DorisTalosLoader(conf);
                this.loadThread = new Thread(new LoadWorker(this.talosLoader), "load_job_audit_talos_loader thread");
                this.loadThread.start();

                isInit = true;
            }
        } catch (Exception e) {
            throw new PluginException(e.getMessage());
        }

    }

    private void loadConfig(PluginContext ctx, Map<String, String> pluginInfoProperties) throws PluginException {
        Path pluginPath = FileSystems.getDefault().getPath(ctx.getPluginPath());
        if (!Files.exists(pluginPath)) {
            throw new PluginException("plugin path does not exist: " + pluginPath);
        }

        Path confFile = pluginPath.resolve("plugin.conf");
        if (!Files.exists(confFile)) {
            throw new PluginException("plugin conf file does not exist: " + confFile);
        }

        final Properties props = new Properties();
        try (InputStream stream = Files.newInputStream(confFile)) {
            props.load(stream);
        } catch (IOException e) {
            throw new PluginException(e.getMessage());
        }

        for (Map.Entry<String, String> entry : pluginInfoProperties.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }

        final Map<String, String> properties = props.stringPropertyNames().stream()
                .collect(Collectors.toMap(Function.identity(), props::getProperty));
        conf = new AuditLoaderConf();
        conf.init(properties);
    }

    @Override
    public void close() throws IOException {
        super.close();
        isClosed = true;
        if (loadThread != null) {
            try {
                loadThread.join();
            } catch (InterruptedException e) {
                LOG.debug("encounter exception when closing the audit loader", e);
            }
        }
    }

    public boolean eventFilter(AuditEvent.EventType type) {
        return type == AuditEvent.EventType.LOAD_SUCCEED;
    }

    public void exec(AuditEvent event) {
        LoadAuditEvent loadAuidtEvent = (LoadAuditEvent) event;
        batchQueue.add(loadAuidtEvent);
    }

    private void assembleAudit(LoadAuditEvent event) {
        JsonObject eventJson = new JsonObject();
        eventJson.add("job_id", new JsonPrimitive(event.jobId));
        eventJson.add("label", new JsonPrimitive(event.label));
        eventJson.add("load_type", new JsonPrimitive(event.loadType));
        eventJson.add("db", new JsonPrimitive(event.db));
        eventJson.add("table_list", new JsonPrimitive(event.tableList));
        eventJson.add("file_path_list", new JsonPrimitive(event.filePathList));
        eventJson.add("broker_user", new JsonPrimitive(event.brokerUser));
        eventJson.add("timestamp", new JsonPrimitive(longToTimeString(event.timestamp)));
        eventJson.add("load_start_time", new JsonPrimitive(longToTimeString(event.loadStartTime)));
        eventJson.add("load_finish_time", new JsonPrimitive(longToTimeString(event.loadFinishTime)));
        eventJson.add("scan_rows", new JsonPrimitive(event.scanRows));
        eventJson.add("scan_bytes", new JsonPrimitive(event.scanBytes));
        eventJson.add("file_number", new JsonPrimitive(event.fileNumber));

        byte[] content = eventJson.toString().getBytes(Charset.forName("UTF-8"));
        Message message = new Message();
        message.setCreateTimestamp(System.currentTimeMillis());
        message.setMessageType(MessageType.BINARY);
        message.setMessage(content);
        auditBuffer.add(message);
        auditBufferSize = auditBufferSize + content.length;
    }

    private void loadIfNecessary(DorisTalosLoader loader) {
        if (auditBufferSize < conf.talosMultiMessageLimit && System.currentTimeMillis() - lastLoadTime < conf.maxBatchIntervalSec * 1000) {
            return;
        }
        lastLoadTime = System.currentTimeMillis();
        try {
            loader.loadBatch(auditBuffer);
        } catch (Exception e) {
            LOG.warn("encounter exception when putting current audit batch, discard current batch", e);
        } finally {
            this.auditBuffer.clear();
            auditBufferSize = 0;
        }
    }

    public static class AuditLoaderConf {
        public static final String PROP_TALOS_MULTI_MESSAGE_LIMIT = "talos_multi_message_limit";
        public static final String PROP_MAX_BATCH_INTERVAL_SEC = "max_batch_interval_sec";
        public static final String PROP_TALOS_SERVICE_ENDPOINT = "talos_service_endpoint";
        public static final String PROP_APP_KEY = "app_key";
        public static final String PROP_APP_SECRET = "app_secret";
        public static final String PROP_TOPIC_NAME = "topic_name";
        public static final String PROP_ORGANIZATION_ID = "organization_id";

        public long talosMultiMessageLimit = 50;
        public long maxBatchIntervalSec = 60;
        public String talosServiceEndpoint = "127.0.0.1:8080";
        public String appKey = "";
        public String appSecret = "";
        public String topicName = "test";
        public String organizationId = "CLXXXX";

        public void init(Map<String, String> properties) throws PluginException {
            try {
               talosMultiMessageLimit = Long.valueOf(properties.getOrDefault(PROP_TALOS_MULTI_MESSAGE_LIMIT,
                       String.valueOf(talosMultiMessageLimit)));
               maxBatchIntervalSec = Long.valueOf(properties.getOrDefault(PROP_MAX_BATCH_INTERVAL_SEC,
                       String.valueOf(maxBatchIntervalSec)));
               talosServiceEndpoint = properties.getOrDefault(PROP_TALOS_SERVICE_ENDPOINT, talosServiceEndpoint);
               appKey = properties.getOrDefault(PROP_APP_KEY, appKey);
               appSecret = properties.getOrDefault(PROP_APP_SECRET, appSecret);
               topicName = properties.getOrDefault(PROP_TOPIC_NAME, topicName);
               organizationId = properties.getOrDefault(PROP_ORGANIZATION_ID, organizationId);
            } catch (Exception e) {
                throw new PluginException(e.getMessage());
            }
        }
    }

    private class LoadWorker implements Runnable {
        private DorisTalosLoader loader;

        public LoadWorker(DorisTalosLoader loader) {
            this.loader = loader;
        }

        public void run() {
            while (!isClosed) {
                try {
                    LoadAuditEvent event = batchQueue.poll(5, TimeUnit.SECONDS);
                    if (event != null) {
                        assembleAudit(event);
                    }
                    loadIfNecessary(loader);
                } catch (InterruptedException e) {
                    LOG.debug("encounter exception when loading current audit batch", e);
                }
            }
            loader.close();
        }
    }

    public static synchronized String longToTimeString(long timeStamp) {
        if (timeStamp <= 0L) {
            return "1900-01-01 00:00:00";
        }
        return DATETIME_FORMAT.format(new Date(timeStamp));
    }
}
