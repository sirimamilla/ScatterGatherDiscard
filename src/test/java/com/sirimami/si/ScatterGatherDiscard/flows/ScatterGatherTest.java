package com.sirimami.si.ScatterGatherDiscard.flows;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowDefinition;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.Optional;
import java.util.concurrent.Executors;

@SpringJUnitConfig
public class ScatterGatherTest {

    @Qualifier("scatterFlow.input")
    @Autowired
    MessageChannel requestChannel;
    @Test
    public void testScatterGatherDiscard(){

        PollableChannel channel = new QueueChannel();
        requestChannel.send(MessageBuilder.withPayload("test")
                .setErrorChannel(channel)
                .setReplyChannel(channel)
                .build());

        Object msg = channel.receive(10_000);


        Assertions.assertThat(msg)
                .isNotNull()
                .isInstanceOf(Message.class)
                .extracting(m -> ((Message) m).getPayload())
                .isInstanceOf(Optional.class)
                .extracting(o -> ((Optional) o).get())
                .isInstanceOf(RuntimeException.class);
    }

    @EnableIntegration
    @Configuration
    public static class ContextConfiguration{

        @Bean
        IntegrationFlow scatterFlow() {
            return f -> f.scatterGather(r -> r.applySequence(true)
                    .recipient("innerFlow.input")
                    .recipientFlow(IntegrationFlowDefinition::bridge),
                    g -> g.groupTimeout(50)
                            .discardChannel("scatterDiscardFlow.input"),
                    sg->sg.gatherTimeout(50));
        }
        @Bean
        public IntegrationFlow innerFlow(){
            return f->f.channel(c->c.executor(Executors.newWorkStealingPool())).transform(m->{
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return m;});
        }

        @Bean
        IntegrationFlow scatterDiscardFlow() {
            return f->f.transform(this, "processDiscardMessage");
        }

        public Message<?> processDiscardMessage(Message<?> msg) {
            return MessageBuilder.withPayload(Optional.of(new RuntimeException("DiscardMessage")))
                    .copyHeaders(msg.getHeaders())
                    .popSequenceDetails()
                    .build();
        }
    }
}
