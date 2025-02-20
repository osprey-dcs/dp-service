package com.ospreydcs.dp.service.common.bson.calculations;

import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

public class CalculationsDocument {

    // instance variables
    private ObjectId id;
    private List<CalculationsDataFrameDocument> dataFrames;

    public ObjectId getId() {
        return id;
    }
    public void setId(ObjectId id) {
        this.id = id;
    }
    public List<CalculationsDataFrameDocument> getDataFrames() {
        return dataFrames;
    }

    public void setDataFrames(List<CalculationsDataFrameDocument> dataFrames) {
        this.dataFrames = dataFrames;
    }

    public static CalculationsDocument fromCalculations(Calculations requestCalculations) {

        final CalculationsDocument calculationsDocument = new CalculationsDocument();

        List<CalculationsDataFrameDocument> dataFrameDocuments = new ArrayList<>();
        for (Calculations.CalculationsDataFrame dataFrame : requestCalculations.getCalculationDataFramesList()) {
            CalculationsDataFrameDocument calculationsDataFrameDocument =
                    CalculationsDataFrameDocument.fromCalculationsDataFrame(dataFrame);
            dataFrameDocuments.add(calculationsDataFrameDocument);
        }
        calculationsDocument.setDataFrames(dataFrameDocuments);

        return calculationsDocument;
    }

    public Calculations toCalculations() throws DpException {
        final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
        for (CalculationsDataFrameDocument dataFrameDocument : getDataFrames()) {
            final Calculations.CalculationsDataFrame dataFrame = dataFrameDocument.toCalculationsDataFrame();
            calculationsBuilder.addCalculationDataFrames(dataFrame);
        }
        return calculationsBuilder.build();
    }
}
