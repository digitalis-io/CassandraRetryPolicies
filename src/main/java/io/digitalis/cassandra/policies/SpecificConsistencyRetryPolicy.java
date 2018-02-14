/***
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
***/
package io.digitalis.cassandra.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision;
import com.datastax.driver.core.exceptions.DriverException;

public final class SpecificConsistencyRetryPolicy implements RetryPolicy {

    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final int DEFAULT_MIN_ALIVE_REPLICA = 1;
    private final ConsistencyLevel retryConsistencyLevel;
    private final int minAliveReplica;
    private final int maxRetryAttempts;


    public SpecificConsistencyRetryPolicy(String retryCL) {
        this(retryCL, SpecificConsistencyRetryPolicy.DEFAULT_MAX_RETRIES, SpecificConsistencyRetryPolicy.DEFAULT_MIN_ALIVE_REPLICA);
    }

    public SpecificConsistencyRetryPolicy(String retryCL, int maxRetryAttempts) {
        this(retryCL, maxRetryAttempts, SpecificConsistencyRetryPolicy.DEFAULT_MIN_ALIVE_REPLICA);
    }

    public SpecificConsistencyRetryPolicy(String retryCL, int maxRetryAttempts, int minAliveReplica) {
        if (maxRetryAttempts < 1 || maxRetryAttempts > 4) {
            throw new IllegalArgumentException("Retry attempts must be a min of 1 and max of 4");
        }
        if (minAliveReplica < 0) {
            throw new IllegalArgumentException("Minimum alive replica count must be >= 0");
        }

        this.retryConsistencyLevel = ConsistencyLevel.valueOf(retryCL);
        this.maxRetryAttempts = maxRetryAttempts;
        this.minAliveReplica = minAliveReplica;
    }

    /**
     * If at least min chosen replica is available or current CL is EACH_QUORUM, retry.
     */
    private RetryDecision chosenCL(int knownOk, ConsistencyLevel currentCL) {

        // JAVA-1005: EACH_QUORUM does not report a global number of alive replicas
        // so even if we get 0 alive replicas, there might be
        // a node up in some other datacenter
        if (knownOk > this.minAliveReplica || currentCL == ConsistencyLevel.EACH_QUORUM)
            return RetryDecision.retry(this.retryConsistencyLevel);
        
        return RetryDecision.rethrow();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation triggers multiple retries. If less replica
     * responded than required by the consistency level (but at least one
     * replica did respond), the operation is retried at the specific
     * consistency level. If enough replica responded but data was not
     * retrieved, the operation is retried with the initial consistency
     * level. Otherwise, an exception is thrown.
     */
    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        if (nbRetry >= this.maxRetryAttempts)
            return RetryDecision.rethrow();

        // CAS reads are not all that useful in terms of visibility of the writes since CAS write supports the
        // normal consistency levels on the committing phase. So the main use case for CAS reads is probably for
        // when you've timed out on a CAS write and want to make sure what happened. Changing CL in that case
        // would be always wrong so we just special case to rethrow.
        if (cl.isSerial())
            return RetryDecision.rethrow();

        if (receivedResponses < requiredResponses) {
            // Tries with the CL that is expected to work
            return chosenCL(receivedResponses, cl);
        }

        return !dataRetrieved ? RetryDecision.retry(cl) : RetryDecision.rethrow();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation triggers multiple retries. If {@code writeType ==
     * WriteType.BATCH_LOG}, the write is retried with the initial
     * consistency level. If {@code writeType == WriteType.UNLOGGED_BATCH}
     * and at least one replica acknowledged, the write is retried with the
     * chosen consistency level (with unlogged batch, a write timeout can
     * <b>always</b> mean that part of the batch haven't been persisted at
     * all, even if {@code receivedAcks > 0}). For write types ({@code WriteType.SIMPLE}
     * we retry at the chosen CL. For {@code WriteType.BATCH}), we retry at the chosen CL.
     * Otherwise, an exception is thrown.
     */
    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, 
                                        int requiredAcks, int receivedAcks, int nbRetry) {
        //Shouldnt happen, but just to be sure
        if (!statement.isIdempotent())
            return RetryDecision.rethrow();

        if (nbRetry >= this.maxRetryAttempts)
            return RetryDecision.rethrow();

        switch (writeType) {
            case SIMPLE:
                return chosenCL(receivedAcks, cl);
            case BATCH:
                // Since we provide atomicity there is no point in retrying
                return chosenCL(receivedAcks, cl);
            case UNLOGGED_BATCH:
                // Since only part of the batch could have been persisted,
                // retry with whatever consistency should allow to persist all
                return chosenCL(receivedAcks, cl);
            case BATCH_LOG:
                return RetryDecision.retry(cl);
        }
        // We want to rethrow on COUNTER and CAS, because in those case "we don't know" and don't want to guess
        return RetryDecision.rethrow();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation triggers a maximum number of chosen retrues. If the minumum replica
     * is know to be alive, the operation is retried at the chosen consistency
     * level.
     */
    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        //Shouldnt happen, but just to be sure
        if (!statement.isIdempotent())
            return RetryDecision.rethrow();
                 
        if (nbRetry >= this.maxRetryAttempts)
            return RetryDecision.rethrow();

        // JAVA-764: if the requested consistency level is serial, it means that the operation failed at the paxos phase of a LWT.
        // Retry on the next host, on the assumption that the initial coordinator could be network-isolated.
        if (cl.isSerial())
            return RetryDecision.tryNextHost(null);

        // Tries the biggest CL that is expected to work
        return chosenCL(aliveReplica, cl);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation triggers a retry on the next host in the query plan
     * with the same consistency level, if the the statement is idempotent.
     */
    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
        //Shouldnt happen, but just to be sure
        if (!statement.isIdempotent())
            return RetryDecision.rethrow();

        return RetryDecision.tryNextHost(cl);
    }

    @Override
    public void init(Cluster cluster) {
        // nothing to do
    }

    @Override
    public void close() {
        // nothing to do
    }
}