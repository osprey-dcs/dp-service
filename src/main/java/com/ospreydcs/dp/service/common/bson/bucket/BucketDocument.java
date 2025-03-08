package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.bson.DpBsonDocumentBase;
import com.ospreydcs.dp.service.common.bson.EventMetadataDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import com.ospreydcs.dp.service.ingest.model.DpIngestionException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This POJO is for writing time series data to mongodb by customizing the code registry.
 *
 * NOTE: DATABASE CODE LIKE insertMany SILENTLY FAILS IF AN INSTANCE VARIABLE IS ADDED WITHOUT ACCESSOR METHODS!!!
 */
public class BucketDocument extends DpBsonDocumentBase {

    private String id;
    private String pvName;
    private EventMetadataDocument eventMetadata;
    private DataColumnDocument dataColumn;
    private DataTimestampsDocument dataTimestamps;
    private Map<String, String> attributeMap;
    private String providerId;
    private String clientRequestId;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPvName() {
        return pvName;
    }

    public void setPvName(String pvName) {
        this.pvName = pvName;
    }

    public EventMetadataDocument getEventMetadata() {
        return eventMetadata;
    }

    public void setEventMetadata(EventMetadataDocument eventMetadata) {
        this.eventMetadata = eventMetadata;
    }

    public DataColumnDocument getDataColumn() {
        return dataColumn;
    }

    public void setDataColumn(DataColumnDocument dataColumn) {
        this.dataColumn = dataColumn;
    }

    public DataTimestampsDocument getDataTimestamps() {
        return dataTimestamps;
    }

    public void setDataTimestamps(DataTimestampsDocument dataTimestamps) {
        this.dataTimestamps = dataTimestamps;
    }

    public Map<String, String> getAttributeMap() {
        return attributeMap;
    }

    public void setAttributeMap(Map<String, String> attributeMap) {
        this.attributeMap = attributeMap;
    }

    public String getProviderId() {
        return providerId;
    }

    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }

    public String getClientRequestId() {
        return clientRequestId;
    }

    public void setClientRequestId(String clientRequestId) {
        this.clientRequestId = clientRequestId;
    }

    /**
     * Generates a list of POJO objects, which are written as a batch to mongodb by customizing the codec registry.
     *
     * NOTE: DATABASE CODE LIKE insertMany SILENTLY FAILS IF AN INSTANCE VARIABLE IS ADDED TO TsDataBucket
     * WITHOUT ACCESSOR METHODS!!!  Very hard to troubleshoot.
     *
     * @param request
     * @return
     */
    public static List<BucketDocument> generateBucketsFromRequest(IngestDataRequest request)
            throws DpIngestionException {

        final List<BucketDocument> bucketList = new ArrayList<>();

        // create DataTimestampsDocument for the request
        final DataTimestampsDocument requestDataTimestampsDocument =
                DataTimestampsDocument.fromDataTimestamps(request.getIngestionDataFrame().getDataTimestamps());

        // create BucketDocument for each column
        final List<DataColumn> columns = request.getIngestionDataFrame().getDataColumnsList();
        for (DataColumn column : columns) {

            final BucketDocument bucket = new BucketDocument();

            // get PV name and generate id for BucketDocument
            final String pvName = column.getName();
            final String documentId = pvName + "-"
                    + requestDataTimestampsDocument.getFirstTime().getSeconds() + "-"
                    + requestDataTimestampsDocument.getFirstTime().getNanos();
            bucket.setId(documentId);
            bucket.setPvName(pvName);
            bucket.setProviderId(request.getProviderId());
            bucket.setClientRequestId(request.getClientRequestId());

            // create DataColumnDocument for request DataColumn
            DataColumnDocument dataColumnDocument = DataColumnDocument.fromDataColumn(column);
            bucket.setDataColumn(dataColumnDocument);

            // embed requestDataTimesetampsDocument within each BucketDocument
            bucket.setDataTimestamps(requestDataTimestampsDocument);

            // add attributes
            final Map<String, String> attributeMap =
                    AttributesUtility.attributeMapFromList(request.getAttributesList());
            bucket.setAttributeMap(attributeMap);

            // create EventMetadataDocument for request EventMetadata
            EventMetadataDocument eventMetadataDocument =
                    EventMetadataDocument.fromEventMetadata(request.getEventMetadata());
            bucket.setEventMetadata(eventMetadataDocument);

            bucketList.add(bucket);
        }

        return bucketList;
    }

    public static QueryDataResponse.QueryData.DataBucket dataBucketFromDocument(
            BucketDocument document
    ) throws DpException {

        final QueryDataResponse.QueryData.DataBucket.Builder bucketBuilder =
                QueryDataResponse.QueryData.DataBucket.newBuilder();

        // add data timestamps
        DataTimestamps dataTimestamps = document.getDataTimestamps().toDataTimestamps();
        bucketBuilder.setDataTimestamps(dataTimestamps);

        // add data values
        DataColumn dataColumn = document.getDataColumn().toDataColumn();
        bucketBuilder.setDataColumn(dataColumn);

        // add attributes
        if (document.getAttributeMap() != null) {
            for (var documentAttributeMapEntry : document.getAttributeMap().entrySet()) {
                final String documentAttributeKey = documentAttributeMapEntry.getKey();
                final String documentAttributeValue = documentAttributeMapEntry.getValue();
                final Attribute responseAttribute = Attribute.newBuilder()
                        .setName(documentAttributeKey)
                        .setValue(documentAttributeValue)
                        .build();
                bucketBuilder.addAttributes(responseAttribute);
            }
        }

        // add event metadata
        if (document.getEventMetadata() != null) {
            final EventMetadataDocument eventMetadataDocument = document.getEventMetadata();
            bucketBuilder.setEventMetadata(eventMetadataDocument.toEventMetadata());
        }

        return bucketBuilder.build();
    }

}
