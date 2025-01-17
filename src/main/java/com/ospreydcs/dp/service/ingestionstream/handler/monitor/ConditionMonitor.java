package com.ospreydcs.dp.service.ingestionstream.handler.monitor;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.common.grpc.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.grpc.TimestampUtility;
import com.ospreydcs.dp.service.ingestionstream.handler.DataEventSubscriptionManager;
import com.ospreydcs.dp.service.ingestionstream.service.IngestionStreamServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConditionMonitor extends EventMonitor {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    public ConditionMonitor(
            SubscribeDataEventRequest request,
            StreamObserver<SubscribeDataEventResponse> responseObserver,
            DataEventSubscriptionManager subscriptionManager
    ) {
        super(request, responseObserver, subscriptionManager);
    }

    @Override
    public void handleSubscribeDataResult(SubscribeDataResponse.SubscribeDataResult result) {

        for (DataColumn dataColumn : result.getDataColumnsList()) {

//            // set up iterator for getting timestamps for column data values
//            final DataTimestampsUtility.DataTimestampsIterator dataTimestampsIterator =
//                    DataTimestampsUtility.dataTimestampsIterator(result.getDataTimestamps());
//            if (dataTimestampsIterator ==  null) {
//                final String errorMsg = "invalid DataTimestamps in SubscribeDataResult";
//                logger.error(errorMsg);
//                handleError(errorMsg);
//                return;
//            }

            int columnValueIndex = 0;
            for (DataValue dataValue : dataColumn.getDataValuesList()) {
//                final Timestamp dataValueTimestamp = dataTimestampsIterator.next();
                final SubscribeDataEventRequest.ConditionEventDef conditionEventDef = request.getConditionEventDef();
                final SubscribeDataEventRequest.ConditionEventDef.ConditionOperator operator =
                        conditionEventDef.getOperator();
                final DataValue operandDataValue = conditionEventDef.getOperandValue();
                switch (dataValue.getValueCase()) {
                    case STRINGVALUE -> {
                        final String errorMsg = "ConditionMonitor does not support DataValue type string";
                        handleError(errorMsg);
                        return;
                    }
                    case BOOLEANVALUE -> {
                        final String errorMsg = "ConditionMonitor does not support DataValue type boolean";
                        handleError(errorMsg);
                        return;
                    }
                    case UINTVALUE -> {
                        final int value = dataValue.getUintValue();
                        final int operandValue = operandDataValue.getUintValue();
                        switch (operator) {
                            case CONDITION_OPERATOR_UNSPECIFIED -> {
                                final String errorMsg = "ConditionEventDef.ConditionOperator must be specified";
                                handleError(errorMsg);
                                return;
                            }
                            case CONDITION_OPERATOR_LESS -> {
                                if (value < operandValue) {
                                    final Timestamp dataValueTimestamp =
                                            DataTimestampsUtility
                                                    .timestampForIndex(result.getDataTimestamps(), columnValueIndex);
                                    if (dataValueTimestamp == null) {
                                        final String errorMsg = "error getting timestamp for column data value";
                                        logger.error(errorMsg);
                                        handleError(errorMsg);
                                        return;
                                    }
                                    IngestionStreamServiceImpl
                                            .sendSubscribeDataEventResponseConditionEvent(
                                                    dataColumn.getName(),
                                                    dataValueTimestamp,
                                                    dataValue,
                                                    responseObserver
                                            );
                                }
                            }
                            case CONDITION_OPERATOR_LESSOREQUAL -> {
                            }
                            case CONDITION_OPERATOR_EQUAL -> {
                            }
                            case CONDITION_OPERATOR_GREATEROREQUAL -> {
                            }
                            case CONDITION_OPERATOR_GREATER -> {
                            }
                            case UNRECOGNIZED -> {
                                final String errorMsg = "ConditionEventDef.ConditionOperator unrecognized enum value";
                                handleError(errorMsg);
                                return;
                            }
                        }
                    }
                    case ULONGVALUE -> {
                    }
                    case INTVALUE -> {
                    }
                    case LONGVALUE -> {
                    }
                    case FLOATVALUE -> {
                    }
                    case DOUBLEVALUE -> {
                    }
                    case BYTEARRAYVALUE -> {
                    }
                    case ARRAYVALUE -> {
                    }
                    case STRUCTUREVALUE -> {
                    }
                    case IMAGEVALUE -> {
                    }
                    case TIMESTAMPVALUE -> {
                    }
                    case VALUE_NOT_SET -> {
                    }
                }
            }
            columnValueIndex = columnValueIndex + 1;
        }
    }


}
