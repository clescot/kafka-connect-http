package io.github.clescot.kafka.connect.http.core.queue;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionEvaluationLogger;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class QueueFactory {
    public static final String DEFAULT_QUEUE_NAME = "default";
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueFactory.class);
    private static final Map<String,Queue> queueMap = Maps.newHashMap();

    private static final Map<String,Boolean> consumers = Maps.newHashMap();

    private QueueFactory(){}

    public static synchronized <T> Queue<T> getQueue(String queueName){
        queueMap.computeIfAbsent(queueName,q->new ConcurrentLinkedQueue<T>());
        return queueMap.get(queueName);
    }
    public static synchronized <T> Queue<T> getQueue(){
        return getQueue(DEFAULT_QUEUE_NAME);
    }

    public static boolean queueMapIsEmpty(){
        return queueMap.isEmpty();
    }

    public static void registerConsumerForQueue(String queueName){
        Preconditions.checkNotNull(queueName,"we cannot register a consumer for a null queueName");
        Preconditions.checkArgument(!queueName.isEmpty(),"we cannot register a consumer for an empty queueName");
        LOGGER.info("registration of a consumer for the queue '{}'",queueName);
        consumers.put(queueName,true);
    }

    private static boolean hasAConsumer(String queueName){
        Boolean queueHasAConsumer = consumers.get(queueName);
        return queueHasAConsumer != null && queueHasAConsumer;
    }

    public static boolean hasAConsumer(String queueName, long maxWaitTimeInMilliSeconds, int pollDelay, int pollInterval){
        Duration maxWaitTimeDuration = Duration.ofMillis(maxWaitTimeInMilliSeconds);
        try {
            Awaitility.await()
                    .atMost(maxWaitTimeDuration)
                    //we are waiting 2 seconds before any check
                    .pollDelay(maxWaitTimeInMilliSeconds> pollDelay ? pollDelay :maxWaitTimeInMilliSeconds-1,TimeUnit.MILLISECONDS)
                    //we check queue consumer every second
                    .pollInterval(pollInterval,TimeUnit.MILLISECONDS)
                    .conditionEvaluationListener(
                            new ConditionEvaluationLogger(
                                    string -> LOGGER.info("awaiting (at max '{}' ms  a registered consumer (Source Connector) listening on the queue : '{}'",maxWaitTimeInMilliSeconds, queueName)
                                    , TimeUnit.SECONDS))
                    //we are waiting at most 'maxWaitTimeDuration' before throwing a timeout exception
                    .atMost(maxWaitTimeDuration)
                    .until(() -> hasAConsumer(queueName));
            LOGGER.info(" registered consumer (Source Connector) is listening on the queue : '{}'",queueName);
        }catch(ConditionTimeoutException e){
            LOGGER.error("no registered consumer (Source Connector) is listening in time on the queue : '{}'",queueName);
            return false;
        }
        return true;
    }

    public static void clearRegistrations() {
        consumers.clear();
    }
    public static void clearQueueMap() {
        queueMap.clear();
    }


}
