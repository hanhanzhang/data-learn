package com.sdu.data.flink.operators;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import com.sdu.data.flink.operators.config.HotConfigDescriptor;
import com.sdu.data.flink.operators.functions.HotUpdateFunction;

public class HotUpdateOperatorFactory<IN, OUT, F extends HotUpdateFunction> implements CoordinatedOperatorFactory<OUT> {

    private final HotConfigDescriptor descriptor;
    private final HotUpdateBaseStreamOperator<OUT, F> streamOperator;
    private ChainingStrategy strategy;

    HotUpdateOperatorFactory(HotConfigDescriptor descriptor, HotUpdateBaseStreamOperator<OUT, F> streamOperator) {
        this.descriptor = descriptor;
        this.streamOperator = streamOperator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
        OperatorID operatorID = parameters.getStreamConfig().getOperatorID();
        parameters.getOperatorEventDispatcher().registerEventHandler(operatorID, streamOperator);
        streamOperator.setProcessingTimeService(parameters.getProcessingTimeService());
        streamOperator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
        return (T) streamOperator;
    }

    @Override
    public void setChainingStrategy(ChainingStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
        return strategy;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return streamOperator.getClass();
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        return new HotUpdateOperatorCoordinatorProvider(operatorID, descriptor);
    }

    private static final class HotUpdateOperatorCoordinatorProvider implements OperatorCoordinator.Provider {

        final OperatorID operatorID;

        final HotConfigDescriptor descriptor;

        HotUpdateOperatorCoordinatorProvider(OperatorID operatorID, HotConfigDescriptor descriptor) {
            this.operatorID = operatorID;
            this.descriptor = descriptor;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        @Override
        public OperatorCoordinator create(OperatorCoordinator.Context context) throws Exception {
            return new HotUpdateOperatorCoordinator(context, descriptor);
        }

    }
}
