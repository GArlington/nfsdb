/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.net.bridge;

import com.lmax.disruptor.*;
import com.nfsdb.journal.concurrent.NamedDaemonThreadFactory;
import com.nfsdb.journal.tx.TxFuture;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.util.Arrays.copyOf;

public class JournalEventBridge {

    private static final AtomicReferenceFieldUpdater<JournalEventBridge, AgentBarrierHolder> AGENT_SEQUENCES_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(JournalEventBridge.class, AgentBarrierHolder.class, "agentBarrierHolder");
    private final ExecutorService executor = Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory("jj-event-bridge", false));
    private final RingBuffer<JournalEvent> inRingBuffer;
    private final BatchEventProcessor<JournalEvent> batchEventProcessor;
    private final RingBuffer<JournalEvent> outRingBuffer;
    private final SequenceBarrier outBarrier;
    private volatile AgentBarrierHolder agentBarrierHolder = new AgentBarrierHolder();


    /**
     * Many-to-many bridge between multiple journals publishing their commits and multiple subscribers consuming them.
     * These subscribers are typically {@link com.nfsdb.journal.net.JournalServerAgent} instances.
     * Disruptor library doesn't provide many-to-many ring buffer out-of-box, so this implementation employs
     * two many-to-one ring buffers, <i>in</i> and <i>out</i>. Single thread is the sole consumer on the <b>in</b> buffer and
     * sole publisher on the <i>out</i> ring buffer.
     * <p>Out ring buffer has timeout blocking wait strategy to enable {@link com.nfsdb.journal.net.JournalServerAgent} to send
     * heartbeat message to client. Value of {@code timeout} is heartbeat frequency between server agent and client.
     *
     * @param timeout         for out buffer wait.
     * @param unit            time unit for timeout value
     * @param eventBufferSize size of ring buffer
     */
    public JournalEventBridge(long timeout, TimeUnit unit, int eventBufferSize) {
        this.inRingBuffer = RingBuffer.createMultiProducer(JournalEvent.EVENT_FACTORY, eventBufferSize, new BlockingWaitStrategy());
        this.outRingBuffer = RingBuffer.createSingleProducer(JournalEvent.EVENT_FACTORY, eventBufferSize, new TimeoutBlockingWaitStrategy(timeout, unit));
        this.outBarrier = outRingBuffer.newBarrier();
        this.batchEventProcessor = new BatchEventProcessor<>(inRingBuffer, inRingBuffer.newBarrier(), new EventHandler<JournalEvent>() {
            @Override
            public void onEvent(JournalEvent event, long sequence, boolean endOfBatch) throws Exception {
                long outSeq = outRingBuffer.next();
                JournalEvent outEvent = outRingBuffer.get(outSeq);
                outEvent.setIndex(event.getIndex());
                outEvent.setTimestamp(event.getTimestamp());
                outRingBuffer.publish(outSeq);
            }
        });
        inRingBuffer.addGatingSequences(batchEventProcessor.getSequence());
    }

    public Sequence createAgentSequence() {
        Sequence sequence = new Sequence(outBarrier.getCursor());
        outRingBuffer.addGatingSequences(sequence);

        AgentBarrierHolder currentHolder;
        AgentBarrierHolder updatedHolder = new AgentBarrierHolder();
        do {
            currentHolder = AGENT_SEQUENCES_UPDATER.get(this);

            updatedHolder.agentSequences = copyOf(currentHolder.agentSequences, currentHolder.agentSequences.length + 1);
            updatedHolder.agentSequences[currentHolder.agentSequences.length] = sequence;
            updatedHolder.barrier = outRingBuffer.newBarrier(updatedHolder.agentSequences);
        }
        while (!AGENT_SEQUENCES_UPDATER.compareAndSet(this, currentHolder, updatedHolder));

        return sequence;
    }

    public TxFuture createRemoteCommitFuture(int journalIndex, long timestamp) {
        return new RemoteCommitFuture(outRingBuffer, agentBarrierHolder.barrier, journalIndex, timestamp);
    }

    public SequenceBarrier getOutBarrier() {
        return outBarrier;
    }

    public RingBuffer<JournalEvent> getOutRingBuffer() {
        return outRingBuffer;
    }

    public void halt() {
        executor.shutdown();
        while (batchEventProcessor.isRunning()) {
            batchEventProcessor.halt();
        }
    }

    public void publish(final int journalIndex, final long timestamp) {
        long sequence = inRingBuffer.next();
        JournalEvent event = inRingBuffer.get(sequence);
        event.setIndex(journalIndex);
        event.setTimestamp(timestamp);
        inRingBuffer.publish(sequence);
    }

    public void removeAgentSequence(Sequence sequence) {
        AgentBarrierHolder currentHolder;
        AgentBarrierHolder updatedHolder = new AgentBarrierHolder();
        do {
            currentHolder = AGENT_SEQUENCES_UPDATER.get(this);
            int toRemove = 0;
            for (int i1 = 0; i1 < currentHolder.agentSequences.length; i1++) {
                if (currentHolder.agentSequences[i1] == sequence) // Specifically uses identity
                {
                    toRemove++;
                }
            }

            if (toRemove == 0) {
                break;
            }

            final int oldSize = currentHolder.agentSequences.length;
            updatedHolder.agentSequences = new Sequence[oldSize - toRemove];

            for (int i = 0, pos = 0; i < oldSize; i++) {
                final Sequence testSequence = currentHolder.agentSequences[i];
                if (sequence != testSequence) {
                    updatedHolder.agentSequences[pos++] = testSequence;
                }
            }

            if (updatedHolder.agentSequences.length > 0) {
                updatedHolder.barrier = outRingBuffer.newBarrier(updatedHolder.agentSequences);
            }
        }
        while (!AGENT_SEQUENCES_UPDATER.compareAndSet(this, currentHolder, updatedHolder));

        outRingBuffer.removeGatingSequence(sequence);
    }

    public void start() {
        executor.submit(batchEventProcessor);
    }
}
