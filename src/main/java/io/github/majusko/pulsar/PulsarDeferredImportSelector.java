package io.github.majusko.pulsar;

import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.DeferredImportSelector;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;


public class PulsarDeferredImportSelector implements DeferredImportSelector, EnvironmentAware {

    private static final String[] NO_IMPORTS = new String[0];
    private Environment environment;

    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        if (this.isEnabled(importingClassMetadata)) {
            return new String[]{
                    "io.github.majusko.pulsar.collector.ConsumerCollector",
                    "io.github.majusko.pulsar.consumer.ConsumerAggregator",
                    "io.github.majusko.pulsar.reactor.FluxConsumerFactory",
                    "io.github.majusko.pulsar.producer.ProducerCollector",
                    "io.github.majusko.pulsar.utils.UrlBuildService"
            };
        }
        return NO_IMPORTS;
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    protected boolean isEnabled(AnnotationMetadata metadata) {
        return environment.getProperty("pulsar.enable", Boolean.class, true);
    }
}