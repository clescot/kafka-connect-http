package com.github.clescot.kafka.connect.http.sink.model;


import com.github.clescot.kafka.connect.http.source.Acknowledgement;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;


@RunWith(Enclosed.class)
public class AcknowledgementTest {
    public static class TestAcknowledgement {

        @Test
        public void test_nominal_case() {
            Acknowledgement acknowledgement = new Acknowledgement( "dfsdfsd","sd897osdmsdg", 200,"toto", Lists.newArrayList(),"nfgnlksdfnlnskdfnlsf","http://toto:8081",Lists.newArrayList(),"PUT","", 100, OffsetDateTime.now(), new AtomicInteger(2));
            Acknowledgement acknowledgement1 = new Acknowledgement( "dfsdfsd","sd897osdmsdg", 200,"toto",Lists.newArrayList(),"nfgnlksdfnlnskdfnlsf","http://toto:8081",Lists.newArrayList(),"PUT","", 100,OffsetDateTime.now(), new AtomicInteger(2));
            acknowledgement1.equals(acknowledgement);
        }
        @Test
        public void test_nominal_case_detail() {
            Acknowledgement acknowledgement = new Acknowledgement( "sdfsfsdf5555", "sd897osdmsdg", 200,"toto",Lists.newArrayList(),"nfgnlksdfnlnskdfnlsf","http://toto:8081",Lists.newArrayList(),"PUT","", 100,OffsetDateTime.now(), new AtomicInteger(2));
            assertThat(acknowledgement.getResponseBody()).isEqualTo("nfgnlksdfnlnskdfnlsf");
            assertThat(acknowledgement.getCorrelationId()).isEqualTo("sdfsfsdf5555");
            assertThat(acknowledgement.getStatusCode()).isEqualTo(200);
        }
    }
}

