package com.sdu.data.flink.operators;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.util.Preconditions;

import com.sdu.data.flink.operators.event.HotUpdateAcknowledgeOperatorEvent;
import com.sdu.data.flink.operators.event.HotUpdateDataAttachOperatorEvent;
import com.sdu.data.flink.operators.functions.HotUpdateFunction;

public abstract class HotUpdateBaseStreamOperator<OUT, F extends HotUpdateFunction>
        extends AbstractUdfStreamOperator<OUT, F>
        implements OperatorEventHandler {

    private OperatorEventGateway operatorGateway;

    public HotUpdateBaseStreamOperator(F userFunction) {
        super(userFunction);
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        Preconditions.checkArgument(operatorGateway != null);
        if (evt instanceof HotUpdateDataAttachOperatorEvent) {
            HotUpdateDataAttachOperatorEvent event = (HotUpdateDataAttachOperatorEvent) evt;
            try {
                userFunction.configChanged(event.getAttachData());
                operatorGateway.sendEventToCoordinator(new HotUpdateAcknowledgeOperatorEvent(event.getEventId(), true, null));
            } catch (Exception e) {
                operatorGateway.sendEventToCoordinator(new HotUpdateAcknowledgeOperatorEvent(event.getEventId(), false, e));
            }
        }
    }

    public void setOperatorGateway(OperatorEventGateway operatorGateway) {
        this.operatorGateway = operatorGateway;
    }
}
