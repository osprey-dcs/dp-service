package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.grpc.v1.ingestionstream.DataEventOperation;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.service.common.model.ResultStatus;

import java.util.List;

public class IngestionStreamValidationUtility {

    public static ResultStatus validateSubscribeDataEventRequest(
            SubscribeDataEventRequest request
    ) {
        // validate common request fields
        if (request.getNewSubscription().getTriggersList().isEmpty()) {
            return new ResultStatus(
                    true,
                    "SubscribeDataEventRequest.triggers must be specified");
        }

        // validate each PvConditionTrigger
        for (PvConditionTrigger pvConditionTrigger : request.getNewSubscription().getTriggersList()) {

            // check that name is specified
            if (pvConditionTrigger.getPvName().isBlank()) {
                return new ResultStatus(
                        true,
                        "SubscribeDataEventRequest PvConditionTrigger.pvName must be specified");
            }

            // check that condition is specified
            if (pvConditionTrigger.getCondition() == PvConditionTrigger.PvCondition.PV_CONDITION_UNSPECIFIED
            ) {
                return new ResultStatus(
                        true,
                        "SubscribeDataEventRequest PvConditionTrigger.condition must be specified");
            }

            // TODO: how to validate that specified pvName is valid

            // TODO: how to validate that PvConditionTrigger value is appropriate for specified pvName and condition,
            // given that we don't know type of pvName
        }

        // validate operation
        if (request.getNewSubscription().hasOperation()) {
            final DataEventOperation dataEventOperation = request.getNewSubscription().getOperation();

            // validate targetPvs list not empty
            final List<String> targetPvs = dataEventOperation.getTargetPvsList();
            if (targetPvs.isEmpty()) {
                return new ResultStatus(
                        true,
                        "SubscribeDataEventRequest DataEventOperation.targetPvs list must not be empty");
            }

            // validate each targetPv
            for (String targetPv : targetPvs) {
                if (targetPv.isBlank()) {
                    return new ResultStatus(
                            true,
                            "SubscribeDataEventRequest DataEventOperation.targetPvs contains empty string");
                }

                // TODO: how to validate that specified targetPv name is valid?
            }

            // validate DataEventWindow
            {
                if (!dataEventOperation.hasWindow()) {
                    return new ResultStatus(
                            true,
                            "SubscribeDataEventRequest DataEventOperation.targetPvs contains empty string");
                }

                final DataEventOperation.DataEventWindow dataEventWindow =
                        dataEventOperation.getWindow();

                // validate window type parameters
                switch (dataEventWindow.getTypeCase()) {

                    // validate DataEventWindow type TimeInterval
                    case TIMEINTERVAL -> {

                        final DataEventOperation.DataEventWindow.TimeInterval timeInterval =
                                dataEventWindow.getTimeInterval();

                        // offset value of zero is allowed, it indicates to start data capture from trigger time

                        // validate that duration is greater than zero
                        if (timeInterval.getDuration() == 0L) {
                            return new ResultStatus(
                                    true,
                                    "SubscribeDataEventRequest.DataEventOperation.DataEventWindow TimeInterval.duration must be specified");
                        }

                        // TODO: do we need to check that duration value is "reasonable", whatever that means?
                    }

// SampleCount is not currently supported, it's not clear how it would work when there are multiple PVs possibly
// using different timescales.
//
//                    // validate DataEventWindow type SampleCount
//                    case SAMPLECOUNT -> {
//
//                        final SubscribeDataEventRequest.DataEventOperation.DataEventWindow.SampleCount sampleCount =
//                                dataEventWindow.getSampleCount();
//
//                        // offset value of zero is allowed, it indicates to start data capture from trigger time
//
//                        // validate that size is greater than zero
//                        if (sampleCount.getSize() == 0L) {
//                            return new ResultStatus(
//                                    true,
//                                    "SubscribeDataEventRequest.DataEventOperation.DataEventWindow SampleCount.size must be specified");
//                        }
//                    }

                    default -> {
                        return new ResultStatus(
                                true,
                                "SubscribeDataEventRequest.DataEventOperation DataEventWindow.type must be specified");
                    }
                }
            }
        }

        return new ResultStatus(false, "");
    }

}
