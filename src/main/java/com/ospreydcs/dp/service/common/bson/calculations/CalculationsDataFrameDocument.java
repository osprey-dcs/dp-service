package com.ospreydcs.dp.service.common.bson.calculations;

import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.grpc.v1.common.DataFrame;
import com.ospreydcs.dp.service.common.bson.column.ColumnDocumentBase;
import com.ospreydcs.dp.service.common.bson.column.ColumnDocumentUtility;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.exception.DpException;

import java.util.List;

public class CalculationsDataFrameDocument {

    String name;
    DataTimestampsDocument dataTimestamps;

    // Polymorphic under the same BSON field name the legacy List<DataColumnDocument> used (#248
    // Phase 4, plan D25): the concrete type round-trips via the _t discriminator, matching the
    // bucket pattern (BucketDocument.dataColumn). Decoding a stored entry that lacks _t fails
    // under an abstract declared type — schema migration v4 stamps pre-1.13 entries.
    List<ColumnDocumentBase> dataColumns;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataTimestampsDocument getDataTimestamps() {
        return dataTimestamps;
    }

    public void setDataTimestamps(DataTimestampsDocument dataTimestamps) {
        this.dataTimestamps = dataTimestamps;
    }

    public List<ColumnDocumentBase> getDataColumns() {
        return dataColumns;
    }

    public void setDataColumns(List<ColumnDocumentBase> dataColumns) {
        this.dataColumns = dataColumns;
    }

    public static CalculationsDataFrameDocument fromCalculationsDataFrame(
            Calculations.CalculationsDataFrame dataFrame
    ) throws DpException {
        CalculationsDataFrameDocument dataFrameDocument = new CalculationsDataFrameDocument();

        // set frame name
        dataFrameDocument.setName(dataFrame.getName());

        // handle DataTimestamps
        DataTimestampsDocument dataTimestampsDocument =
                DataTimestampsDocument.fromDataTimestamps(dataFrame.getFrame().getDataTimestamps());
        dataFrameDocument.setDataTimestamps(dataTimestampsDocument);

        // handle columns of all types via the shared DataFrame dispatch
        dataFrameDocument.setDataColumns(ColumnDocumentUtility.fromDataFrame(dataFrame.getFrame()));

        return dataFrameDocument;
    }

    public Calculations.CalculationsDataFrame toCalculationsDataFrame() throws DpException {

        final Calculations.CalculationsDataFrame.Builder dataFrameBuilder =
                Calculations.CalculationsDataFrame.newBuilder();

        dataFrameBuilder.setName(getName());

        final DataFrame.Builder frameBuilder = DataFrame.newBuilder();

        frameBuilder.setDataTimestamps(this.dataTimestamps.toDataTimestamps());

        for (ColumnDocumentBase columnDocument : this.dataColumns) {
            ColumnDocumentUtility.addColumnToDataFrame(frameBuilder, columnDocument);
        }

        dataFrameBuilder.setFrame(frameBuilder);

        return dataFrameBuilder.build();
    }

}
