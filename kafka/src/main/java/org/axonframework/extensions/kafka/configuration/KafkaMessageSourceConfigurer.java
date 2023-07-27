/*
 * Copyright (c) 2010-2021. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.kafka.configuration;

import org.axonframework.config.Component;
import org.axonframework.config.Configuration;
import org.axonframework.config.ModuleConfiguration;
import org.axonframework.extensions.kafka.eventhandling.consumer.subscribable.SubscribableKafkaMessageSource;
import org.axonframework.lifecycle.Phase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.axonframework.lifecycle.Phase.INBOUND_EVENT_CONNECTORS;

/**
 * A {@link ModuleConfiguration} to configure Kafka as a message source for {@link
 * org.axonframework.eventhandling.EventProcessor} instances. This ModuleConfiguration should be registered towards the
 * {@link org.axonframework.config.Configurer} for it to start amy Kafka sources for an EventProcessor.
 *
 * @author Steven van Beelen
 * @since 4.0
 */
public class KafkaMessageSourceConfigurer implements ModuleConfiguration {

    private Configuration config;
    private final List<Component<SubscribableKafkaMessageSource<?, ?>>> sources = new ArrayList<>();

    @Override
    public void initialize(Configuration config) {
        this.config = config;

        if (!sources.isEmpty()) {
            this.config.onStart(
                    INBOUND_EVENT_CONNECTORS,
                    () -> sources.stream().map(Component::get)
                            .forEach(SubscribableKafkaMessageSource::start)
            );
            this.config.onShutdown(
                    INBOUND_EVENT_CONNECTORS,
                    () -> sources.stream().map(Component::get)
                            .forEach(SubscribableKafkaMessageSource::close)
            );
        }
    }

    /**
     * Register a {@link Function} which uses the provided {@link Configuration} to build a {@link
     * SubscribableKafkaMessageSource}.
     *
     * @param subscribableKafkaMessageSource the {@link Function} which will build a {@link SubscribableKafkaMessageSource}
     */
    public void configureSubscribableSource(
            Function<Configuration, SubscribableKafkaMessageSource<?, ?>> subscribableKafkaMessageSource
    ) {
        sources.add(new Component<>(
                () -> config, "subscribableKafkaMessageSource", subscribableKafkaMessageSource
        ));
    }
}