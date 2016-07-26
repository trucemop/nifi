package org.apache.nifi.accumulo;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

public class PutAccumuloObject extends AbstractProcessor {

    static final AllowableValue DURABILITY_DEFAULT = new AllowableValue(Durability.DEFAULT.name(), "System Default", "The data is stored using the Table or System's default configuration");
    static final AllowableValue DURABILITY_NONE = new AllowableValue(Durability.NONE.name(), "None", "The data is transferred to 'success' without waiting for confirmation from Accumulo");
    static final AllowableValue DURABILITY_FLUSH = new AllowableValue(Durability.FLUSH.name(), "Flush",
            "The data is transferred to 'success' after Accumulo confirms that it has received the data, but the data may not be persisted");
    static final AllowableValue DURABILITY_LOG = new AllowableValue(Durability.LOG.name(), "Log",
            "The data is transferred to 'success' after Accumulo has received the data, but the data may not yet be replicated");
    static final AllowableValue DURABILITY_SYNC = new AllowableValue(Durability.SYNC.name(), "Sync", "The data is transferred to 'success' only after Accumulo confirms that it has been stored");

    static final PropertyDescriptor INSTANCE_NAME = new PropertyDescriptor.Builder()
            .name("Accumulo Instance Name")
            .description("The name of the Accumulo Instance to connect to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor ZOOKEEPER_CONNECT_STRING = new PropertyDescriptor.Builder()
            .name("ZooKeeper Connection String")
            .description("A comma-separated list of ZooKeeper hostname:port pairs")
            .required(true)
            .defaultValue("localhost:2181")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username to use when connecting to Accumulo")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password to use when connecting to Accumulo")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor OBJECT_TABLE = new PropertyDescriptor.Builder()
            .name("Object Table")
            .description("The table in Accumulo to write object contents")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DIRECTORY_TABLE = new PropertyDescriptor.Builder()
            .name("Directory Table")
            .description("The table in Accumulo to write directory contents")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor INDEX_TABLE = new PropertyDescriptor.Builder()
            .name("Index Table")
            .description("The table in Accumulo to write directory contents")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor VISIBILITY_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Visibility Attribute")
            .description("The attribute containing visibility for this Accumulo cell")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor CHUNK_SIZE = new PropertyDescriptor.Builder()
            .name("Chunk Size")
            .description("The size of chunks when breaking down files")
            .required(true)
            .expressionLanguageSupported(false)
            .defaultValue("100000")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor OBJECT_NAME_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Object Name Attribute")
            .description("The attribute containing the name of this object.  Path elements separated by a forward slash in the value.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor OBJECT_ID_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Object ID Attribute")
            .description("The attribute containing this object's ID.  Object contents written to the data table will be deduped based "
                    + "on this value.  Commonly a hash of the Object's contents.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor OBJECT_NAME_ID_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Object Name ID Attribute")
            .description("The attribute containing the ID for this object's name.  The metadata elements of duplicate Object entries in the "
                    + "data table are grouped by this ID so that multiple Object names can retain their original metadata values.  Commonly a "
                    + "hash of the Object Name.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor TIMESTAMP_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Timestamp Attribute")
            .description("The attribute containing the timestamp for this object.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor PATH_SEP = new PropertyDescriptor.Builder()
            .name("Path separator")
            .description("String to use as a separator for the Object's path.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Communications Timeout")
            .description("Specifies how long to wait without receiving a response from Accumulo before routing FlowFiles to 'failure'")
            .required(true)
            .defaultValue("30 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("Number of FlowFiles to send in a single batch. Accumulo does not provide information about which data fails in the case of a batch operation. "
                    + "Therefore, if any FlowFile fails in the batch, all may be routed to failure, even if they were already sent successfully to Accumulo. "
                    + "If this is problematic for your use case, use a Batch Size of 1.")
            .required(true)
            .expressionLanguageSupported(false)
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor DURABILITY = new PropertyDescriptor.Builder()
            .name("Data Durability")
            .description("Specifies how durably the data must be stored on Accumulo before sending a FlowFile to the 'success' relationship.")
            .required(true)
            .allowableValues(DURABILITY_DEFAULT, DURABILITY_NONE, DURABILITY_FLUSH, DURABILITY_LOG, DURABILITY_SYNC)
            .defaultValue(DURABILITY_DEFAULT.getValue())
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been sent to Accumulo")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to Accumulo")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(INSTANCE_NAME);
        properties.add(ZOOKEEPER_CONNECT_STRING);
        properties.add(USERNAME);
        properties.add(PASSWORD);
        properties.add(OBJECT_TABLE);
        properties.add(DIRECTORY_TABLE);
        properties.add(INDEX_TABLE);
        properties.add(VISIBILITY_ATTRIBUTE);
        properties.add(CHUNK_SIZE);
        properties.add(OBJECT_NAME_ATTRIBUTE);
        properties.add(OBJECT_ID_ATTRIBUTE);
        properties.add(OBJECT_NAME_ID_ATTRIBUTE);
        properties.add(TIMESTAMP_ATTRIBUTE);
        properties.add(PATH_SEP);
        properties.add(DURABILITY);
        properties.add(BATCH_SIZE);
        properties.add(TIMEOUT);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    private Connector getConnector(final ProcessContext context) throws AccumuloException, AccumuloSecurityException {
        final String instanceName = context.getProperty(INSTANCE_NAME).getValue();
        final String zookeeperConnString = context.getProperty(ZOOKEEPER_CONNECT_STRING).getValue();
        final Instance instance = new ZooKeeperInstance(instanceName, zookeeperConnString);

        final String username = context.getProperty(USERNAME).getValue();
        final String password = context.getProperty(PASSWORD).getValue();
        return instance.getConnector(username, new PasswordToken(password));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(context.getProperty(BATCH_SIZE).asInteger());
        if (flowFiles.isEmpty()) {
            return;
        }

        try {
            final String durability = context.getProperty(DURABILITY).getValue();
            final int chunkSize = context.getProperty(CHUNK_SIZE).asInteger();
            final Connector connector = getConnector(context);
            final BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
            batchWriterConfig.setDurability(Durability.valueOf(durability.toUpperCase()));
            batchWriterConfig.setTimeout(context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);

            final List<FlowFile> success = new ArrayList<>(flowFiles.size());

            final MultiTableBatchWriter writer = connector.createMultiTableBatchWriter(batchWriterConfig);
            try {
                for (final FlowFile flowFile : flowFiles) {
                    final String objectTableName = context.getProperty(OBJECT_TABLE).evaluateAttributeExpressions(flowFile).getValue();
                    final String directoryTableName = context.getProperty(DIRECTORY_TABLE).evaluateAttributeExpressions(flowFile).getValue();
                    final String indexTableName = context.getProperty(INDEX_TABLE).evaluateAttributeExpressions(flowFile).getValue();
                    final String objectNameKey = context.getProperty(OBJECT_NAME_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
                    final String objectIdKey = context.getProperty(OBJECT_ID_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
                    final String objectNameIdKey = context.getProperty(OBJECT_NAME_ID_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
                    final String timestampKey = context.getProperty(TIMESTAMP_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
                    final String pathSep = context.getProperty(PATH_SEP).getValue();
                    final String objectName = flowFile.getAttribute(objectNameKey);
                    final String objectId = flowFile.getAttribute(objectIdKey);

                    final long timestamp = Long.parseLong(flowFile.getAttribute(timestampKey));

                    final BatchWriter objectBw;
                    try {
                        objectBw = writer.getBatchWriter(objectTableName);
                    } catch (final TableNotFoundException e) {
                        getLogger().error("Failed to send {} to Accumulo because the table {} is not known; routing to failure", new Object[]{flowFile, objectTableName});
                        session.transfer(flowFile, REL_FAILURE);
                        continue;
                    }

                    final BatchWriter directoryBw;
                    try {
                        directoryBw = writer.getBatchWriter(directoryTableName);
                    } catch (final TableNotFoundException e) {
                        getLogger().error("Failed to send {} to Accumulo because the table {} is not known; routing to failure", new Object[]{flowFile, directoryTableName});
                        session.transfer(flowFile, REL_FAILURE);
                        continue;
                    }

                    final BatchWriter indexBw;
                    try {
                        indexBw = writer.getBatchWriter(indexTableName);
                    } catch (final TableNotFoundException e) {
                        getLogger().error("Failed to send {} to Accumulo because the table {} is not known; routing to failure", new Object[]{flowFile, indexTableName});
                        session.transfer(flowFile, REL_FAILURE);
                        continue;
                    }

                    success.add(flowFile);

                    final String visibilityKey = flowFile.getAttribute(context.getProperty(OBJECT_TABLE).evaluateAttributeExpressions(flowFile).getValue());

                    final String visString = flowFile.getAttribute(visibilityKey);
                    ColumnVisibility cv = null;
                    if (visString == null) {
                        cv = new ColumnVisibility();
                    } else {
                        cv = new ColumnVisibility(visString);
                    }

                    final ObjectIngest ingest = new ObjectIngest(chunkSize, pathSep, cv);

                    final Map<String, String> refMap = flowFile.getAttributes();

                    session.read(flowFile, new InputStreamCallback() {
                        @Override
                        public void process(final InputStream in) throws IOException {
                            try {
                                ingest.insertObjectData(objectIdKey, objectNameIdKey, timestamp, refMap, in, objectBw);
                            } catch (MutationsRejectedException ex) {
                                getLogger().error("Failed to write {} to the {} Accumulo table; routing to failure", new Object[]{flowFile, objectTableName});
                                session.transfer(flowFile, REL_FAILURE);
                            }
                        }
                    });

                    directoryBw.addMutations(ingest.buildDirectoryMutations(objectNameKey, timestamp, refMap));
                    indexBw.addMutations(ingest.buildIndexMutations(objectNameKey, timestamp, refMap));

                    session.getProvenanceReporter().send(flowFile, "accumulo://" + objectTableName + "/" + directoryTableName + "/" + indexTableName + ":" + objectName + ":" + objectId);
                }
            } finally {
                writer.close();
            }

            getLogger().info("Successfully transferred {} FlowFiles to success", new Object[]{success.size()});
            session.transfer(success, REL_SUCCESS);
        } catch (final AccumuloException | AccumuloSecurityException e) {
            getLogger().error("Failed to send {} FlowFiles to Accumulo due to {}; routing to failure", new Object[]{flowFiles.size(), e});
            session.transfer(flowFiles, REL_FAILURE);
            return;
        }
    }
}
