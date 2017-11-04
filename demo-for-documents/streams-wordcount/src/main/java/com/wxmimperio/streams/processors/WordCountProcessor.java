package com.wxmimperio.streams.processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Locale;

/**
 * Created by wxmimperio on 2017/11/5.
 */
public class WordCountProcessor implements Processor<String, String>, ProcessorSupplier {
    private ProcessorContext context;
    private KeyValueStore<String, Integer> kvStore;

    @Override
    public Processor<String, String> get() {
        return new WordCountProcessor();
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.context.schedule(1000);
        this.kvStore = (KeyValueStore<String, Integer>) context.getStateStore("Counts");
    }

    @Override
    public void process(String key, String value) {
        String[] words = value.toLowerCase(Locale.getDefault()).split(" ");

        for (String word : words) {
            Integer oldValue = this.kvStore.get(word);
            if (oldValue == null) {
                this.kvStore.put(word, 1);
            } else {
                this.kvStore.put(word, oldValue + 1);
            }
        }
        context.commit();
    }

    @Override
    public void punctuate(long timestamp) {
        try (KeyValueIterator<String, Integer> iterator = this.kvStore.all()) {
            System.out.println("----------- " + timestamp + " ----------- ");
            while (iterator.hasNext()) {
                KeyValue<String, Integer> entry = iterator.next();
                System.out.println("[" + entry.key + ", " + entry.value + "]");
                context.forward(entry.key, entry.value.toString());
            }
        }
    }

    @Override
    public void close() {
        context.commit();
    }
}
