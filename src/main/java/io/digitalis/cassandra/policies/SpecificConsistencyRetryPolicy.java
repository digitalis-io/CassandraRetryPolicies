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
    private final ConsistencyLevel retryConsistencyLevel;
    private final int maxRetryAttempts;


    public SpecificConsistencyRetryPolicy(String retryCL) {
        this(retryCL, SpecificConsistencyRetryPolicy.DEFAULT_MAX_RETRIES);
    }


    public SpecificConsistencyRetryPolicy(String retryCL, int maxRetryAttempts) {
        if (maxRetryAttempts < 1) {
            throw new IllegalArgumentException("Retry attempts must be a > 1");
        }

        this.retryConsistencyLevel = ConsistencyLevel.valueOf(retryCL);
        this.maxRetryAttempts = (maxRetryAttempts + 1); //Account for network blip retry attempts
    }


    private RetryDecision chosenCL(int retryAttempt, ConsistencyLevel currentCL) {
        if (retryAttempt <= 1) {
            // Retry on the next host, on the assumption that the initial coordinator could be network-isolated.
            return RetryDecision.tryNextHost(currentCL);
        } else if (retryAttempt >= this.maxRetryAttempts) {
            return RetryDecision.rethrow();
        }
        
        return RetryDecision.retry(this.retryConsistencyLevel);
    }


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

        return chosenCL(nbRetry, cl);
    }


    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, 
                                        int requiredAcks, int receivedAcks, int nbRetry) {

        if (nbRetry >= this.maxRetryAttempts)
            return RetryDecision.rethrow();

        switch (writeType) {
            case SIMPLE:
                return chosenCL(nbRetry, cl);
            case BATCH:
                // Since we provide atomicity there is no point in retrying
                return chosenCL(nbRetry, cl);
            case UNLOGGED_BATCH:
                // Since only part of the batch could have been persisted,
                // retry with whatever consistency should allow to persist all
                return chosenCL(nbRetry, cl);
            case BATCH_LOG:
                return RetryDecision.retry(cl);
        }
        // We want to rethrow on COUNTER and CAS, because in those case "we don't know" and don't want to guess
        return RetryDecision.rethrow();
    }


    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
                 
        if (nbRetry >= this.maxRetryAttempts)
            return RetryDecision.rethrow();

        // JAVA-764: if the requested consistency level is serial, it means that the operation failed at the paxos phase of a LWT.
        // Retry on the next host, on the assumption that the initial coordinator could be network-isolated.
        if (cl.isSerial())
            return RetryDecision.tryNextHost(null);

        // Tries the biggest CL that is expected to work
        return chosenCL(nbRetry, cl);
    }


    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {

        if (nbRetry >= this.maxRetryAttempts)
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