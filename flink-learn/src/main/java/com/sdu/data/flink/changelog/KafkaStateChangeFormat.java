package com.sdu.data.flink.changelog;

import static java.util.Comparator.comparing;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStateChangeFormat {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStateChangeFormat.class);

     Map<KafkaStateChangeSet, Tuple2<Long, Long>> write(
            KafkaOutputStream out,
            Collection<KafkaStateChangeSet> changeSets) throws IOException {
         List<KafkaStateChangeSet> sorted = new ArrayList<>(changeSets);
         // using sorting instead of bucketing for simplicity
         sorted.sort(
                 comparing(KafkaStateChangeSet::getLogId)
                         .thenComparing(KafkaStateChangeSet::getSequenceNumber));

         throw new RuntimeException("");
    }

    CloseableIterator<KafkaStateChange> read(DataInputStream input) throws IOException {
         return new CloseableIterator<KafkaStateChange>() {

             // KeyGroup length
             int numUnreadGroups = input.readInt();
             //
             int numLeftInGroup = numUnreadGroups-- == 0 ? 0 : input.readInt();

             int keyGroup = numLeftInGroup == 0 ? 0 : input.readInt();

             @Override
             public void close() throws Exception {

             }

             @Override
             public boolean hasNext() {
                 return false;
             }

             @Override
             public KafkaStateChange next() {
                 return null;
             }
         };
    }
}
