package com.microsoft.azure.servicebus.primitives;

import com.microsoft.azure.servicebus.rules.RuleDescription;
import java8.util.concurrent.CompletableFuture;
import java8.util.concurrent.CompletionStage;
import java8.util.function.BiFunction;
import java8.util.function.Function;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class MiscRequestResponseOperationHandler extends ClientEntity {
    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(MiscRequestResponseOperationHandler.class);

    private final Object requestResonseLinkCreationLock = new Object();
    private final String entityPath;
    private final MessagingEntityType entityType;
    private final MessagingFactory underlyingFactory;
    private RequestResponseLink requestResponseLink;
    private CompletableFuture<Void> requestResponseLinkCreationFuture;

    private MiscRequestResponseOperationHandler(MessagingFactory factory, String linkName, String entityPath, MessagingEntityType entityType) {
        super(linkName);

        this.underlyingFactory = factory;
        this.entityPath = entityPath;
        this.entityType = entityType;
    }

    @Deprecated
    public static CompletableFuture<MiscRequestResponseOperationHandler> create(MessagingFactory factory, String entityPath) {
        return create(factory, entityPath, null);
    }

    public static CompletableFuture<MiscRequestResponseOperationHandler> create(MessagingFactory factory, String entityPath, MessagingEntityType entityType) {
        MiscRequestResponseOperationHandler requestResponseOperationHandler = new MiscRequestResponseOperationHandler(factory, StringUtil.getShortRandomString(), entityPath, entityType);
        return CompletableFuture.completedFuture(requestResponseOperationHandler);
    }

    private void closeInternals() {
        this.closeRequestResponseLink();
    }

    @Override
    protected CompletableFuture<Void> onClose() {
        TRACE_LOGGER.trace("Closing MiscRequestResponseOperationHandler");
        this.closeInternals();
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> createRequestResponseLink() {
        synchronized (this.requestResonseLinkCreationLock) {
            if (this.requestResponseLinkCreationFuture == null) {
                this.requestResponseLinkCreationFuture = new CompletableFuture<Void>();
                this.underlyingFactory.obtainRequestResponseLinkAsync(this.entityPath, this.entityType).handleAsync(new BiFunction<RequestResponseLink, Throwable, Object>() {
                    @Override
                    public Object apply(RequestResponseLink rrlink, Throwable ex) {
                        if (ex == null) {
                            MiscRequestResponseOperationHandler.this.requestResponseLink = rrlink;
                            MiscRequestResponseOperationHandler.this.requestResponseLinkCreationFuture.complete(null);
                        } else {
                            Throwable cause = ExceptionUtil.extractAsyncCompletionCause(ex);
                            MiscRequestResponseOperationHandler.this.requestResponseLinkCreationFuture.completeExceptionally(cause);
                            // Set it to null so next call will retry rr link creation
                            synchronized (MiscRequestResponseOperationHandler.this.requestResonseLinkCreationLock) {
                                MiscRequestResponseOperationHandler.this.requestResponseLinkCreationFuture = null;
                            }
                        }
                        return null;
                    }
                }, MessagingFactory.INTERNAL_THREAD_POOL);
            }

            return this.requestResponseLinkCreationFuture;
        }
    }

    private void closeRequestResponseLink() {
        synchronized (this.requestResonseLinkCreationLock) {
            if (this.requestResponseLinkCreationFuture != null) {
                this.requestResponseLinkCreationFuture.thenRun(new Runnable() {
                    @Override
                    public void run() {
                        MiscRequestResponseOperationHandler.this.underlyingFactory.releaseRequestResponseLink(MiscRequestResponseOperationHandler.this.entityPath);
                        MiscRequestResponseOperationHandler.this.requestResponseLink = null;
                    }
                });
                this.requestResponseLinkCreationFuture = null;
            }
        }
    }

    public CompletableFuture<Pair<String[], Integer>> getMessageSessionsAsync(final Date lastUpdatedTime, final int skip, final int top, final String lastSessionId) {
        TRACE_LOGGER.debug("Getting message sessions from entity '{}' with lastupdatedtime '{}', skip '{}', top '{}', lastsessionid '{}'", this.entityPath, lastUpdatedTime, skip, top, lastSessionId);
        return this.createRequestResponseLink().thenComposeAsync(new Function<Void, CompletionStage<Pair<String[], Integer>>>() {
            @Override
            public CompletionStage<Pair<String[], Integer>> apply(Void v) {
                HashMap requestBodyMap = new HashMap();
                requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_LAST_UPDATED_TIME, lastUpdatedTime);
                requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_SKIP, skip);
                requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_TOP, top);
                if (lastSessionId != null) {
                    requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_LAST_SESSION_ID, lastSessionId);
                }

                Message requestMessage = RequestResponseUtils.createRequestMessageFromPropertyBag(ClientConstants.REQUEST_RESPONSE_GET_MESSAGE_SESSIONS_OPERATION, requestBodyMap, Util.adjustServerTimeout(MiscRequestResponseOperationHandler.this.underlyingFactory.getOperationTimeout()));
                CompletableFuture<Message> responseFuture = MiscRequestResponseOperationHandler.this.requestResponseLink.requestAysnc(requestMessage, MiscRequestResponseOperationHandler.this.underlyingFactory.getOperationTimeout());
                return responseFuture.thenComposeAsync(new Function<Message, CompletableFuture<Pair<String[], Integer>>>() {
                    @Override
                    public CompletableFuture<Pair<String[], Integer>> apply(Message responseMessage) {
                        CompletableFuture<Pair<String[], Integer>> returningFuture = new CompletableFuture<Pair<String[], Integer>>();
                        int statusCode = RequestResponseUtils.getResponseStatusCode(responseMessage);
                        if (statusCode == ClientConstants.REQUEST_RESPONSE_OK_STATUS_CODE) {
                            Map responseBodyMap = RequestResponseUtils.getResponseBody(responseMessage);
                            int responseSkip = (int) responseBodyMap.get(ClientConstants.REQUEST_RESPONSE_SKIP);
                            String[] sessionIds = (String[]) responseBodyMap.get(ClientConstants.REQUEST_RESPONSE_SESSIONIDS);
                            TRACE_LOGGER.debug("Received '{}' sessions from entity '{}'. Response skip '{}'", sessionIds.length, MiscRequestResponseOperationHandler.this.entityPath, responseSkip);
                            returningFuture.complete(new Pair<>(sessionIds, responseSkip));
                        } else if (statusCode == ClientConstants.REQUEST_RESPONSE_NOCONTENT_STATUS_CODE ||
                                (statusCode == ClientConstants.REQUEST_RESPONSE_NOTFOUND_STATUS_CODE && ClientConstants.SESSION_NOT_FOUND_ERROR.equals(RequestResponseUtils.getResponseErrorCondition(responseMessage)))) {
                            TRACE_LOGGER.debug("Received no sessions from entity '{}'.", MiscRequestResponseOperationHandler.this.entityPath);
                            returningFuture.complete(new Pair<>(new String[0], 0));
                        } else {
                            // error response
                            TRACE_LOGGER.debug("Receiving sessions from entity '{}' failed with status code '{}'", MiscRequestResponseOperationHandler.this.entityPath, statusCode);
                            returningFuture.completeExceptionally(RequestResponseUtils.genereateExceptionFromResponse(responseMessage));
                        }
                        return returningFuture;
                    }
                }, MessagingFactory.INTERNAL_THREAD_POOL);
            }
        }, MessagingFactory.INTERNAL_THREAD_POOL);
    }

    public CompletableFuture<Void> removeRuleAsync(final String ruleName) {
        TRACE_LOGGER.debug("Removing rule '{}' from entity '{}'", ruleName, this.entityPath);
        return this.createRequestResponseLink().thenComposeAsync(new Function<Void, CompletionStage<Void>>() {
            @Override
            public CompletionStage<Void> apply(Void v) {
                HashMap requestBodyMap = new HashMap();
                requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_RULENAME, ruleName);

                Message requestMessage = RequestResponseUtils.createRequestMessageFromPropertyBag(ClientConstants.REQUEST_RESPONSE_REMOVE_RULE_OPERATION, requestBodyMap, Util.adjustServerTimeout(MiscRequestResponseOperationHandler.this.underlyingFactory.getOperationTimeout()));
                CompletableFuture<Message> responseFuture = MiscRequestResponseOperationHandler.this.requestResponseLink.requestAysnc(requestMessage, MiscRequestResponseOperationHandler.this.underlyingFactory.getOperationTimeout());
                return responseFuture.thenComposeAsync(new Function<Message, CompletionStage<Void>>() {
                    @Override
                    public CompletionStage<Void> apply(Message responseMessage) {
                        CompletableFuture<Void> returningFuture = new CompletableFuture<Void>();
                        int statusCode = RequestResponseUtils.getResponseStatusCode(responseMessage);
                        if (statusCode == ClientConstants.REQUEST_RESPONSE_OK_STATUS_CODE) {
                            TRACE_LOGGER.debug("Removed rule '{}' from entity '{}'", ruleName, MiscRequestResponseOperationHandler.this.entityPath);
                            returningFuture.complete(null);
                        } else {
                            // error response
                            TRACE_LOGGER.error("Removing rule '{}' from entity '{}' failed with status code '{}'", ruleName, MiscRequestResponseOperationHandler.this.entityPath, statusCode);
                            returningFuture.completeExceptionally(RequestResponseUtils.genereateExceptionFromResponse(responseMessage));
                        }
                        return returningFuture;
                    }
                }, MessagingFactory.INTERNAL_THREAD_POOL);
            }
        }, MessagingFactory.INTERNAL_THREAD_POOL);
    }

    public CompletableFuture<Void> addRuleAsync(final RuleDescription ruleDescription) {
        TRACE_LOGGER.debug("Adding rule '{}' to entity '{}'", ruleDescription.getName(), this.entityPath);
        return this.createRequestResponseLink().thenComposeAsync(new Function<Void, CompletionStage<Void>>() {
            @Override
            public CompletionStage<Void> apply(Void v) {
                HashMap requestBodyMap = new HashMap();
                requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_RULENAME, ruleDescription.getName());
                requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_RULEDESCRIPTION, RequestResponseUtils.encodeRuleDescriptionToMap(ruleDescription));

                Message requestMessage = RequestResponseUtils.createRequestMessageFromPropertyBag(ClientConstants.REQUEST_RESPONSE_ADD_RULE_OPERATION, requestBodyMap, Util.adjustServerTimeout(MiscRequestResponseOperationHandler.this.underlyingFactory.getOperationTimeout()));
                CompletableFuture<Message> responseFuture = MiscRequestResponseOperationHandler.this.requestResponseLink.requestAysnc(requestMessage, MiscRequestResponseOperationHandler.this.underlyingFactory.getOperationTimeout());
                return responseFuture.thenComposeAsync(new Function<Message, CompletionStage<Void>>() {
                    @Override
                    public CompletionStage<Void> apply(Message responseMessage) {
                        CompletableFuture<Void> returningFuture = new CompletableFuture<Void>();
                        int statusCode = RequestResponseUtils.getResponseStatusCode(responseMessage);
                        if (statusCode == ClientConstants.REQUEST_RESPONSE_OK_STATUS_CODE) {
                            TRACE_LOGGER.debug("Added rule '{}' to entity '{}'", ruleDescription.getName(), MiscRequestResponseOperationHandler.this.entityPath);
                            returningFuture.complete(null);
                        } else {
                            // error response
                            TRACE_LOGGER.error("Adding rule '{}' to entity '{}' failed with status code '{}'", ruleDescription.getName(), MiscRequestResponseOperationHandler.this.entityPath, statusCode);
                            returningFuture.completeExceptionally(RequestResponseUtils.genereateExceptionFromResponse(responseMessage));
                        }
                        return returningFuture;
                    }
                }, MessagingFactory.INTERNAL_THREAD_POOL);
            }
        }, MessagingFactory.INTERNAL_THREAD_POOL);
    }

    public CompletableFuture<Collection<RuleDescription>> getRulesAsync(final int skip, final int top) {
        TRACE_LOGGER.debug("Fetching rules for entity '{}'", this.entityPath);
        return this.createRequestResponseLink().thenComposeAsync(new Function<Void, CompletionStage<Collection<RuleDescription>>>() {
            @Override
            public CompletionStage<Collection<RuleDescription>> apply(Void v) {
                HashMap requestBodyMap = new HashMap();
                requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_SKIP, skip);
                requestBodyMap.put(ClientConstants.REQUEST_RESPONSE_TOP, top);

                Message requestMessage = RequestResponseUtils.createRequestMessageFromPropertyBag(
                        ClientConstants.REQUEST_RESPONSE_GET_RULES_OPERATION,
                        requestBodyMap,
                        Util.adjustServerTimeout(MiscRequestResponseOperationHandler.this.underlyingFactory.getOperationTimeout()));
                CompletableFuture<Message> responseFuture = MiscRequestResponseOperationHandler.this.requestResponseLink.requestAysnc(requestMessage, MiscRequestResponseOperationHandler.this.underlyingFactory.getOperationTimeout());
                return responseFuture.thenComposeAsync(new Function<Message, CompletionStage<Collection<RuleDescription>>>() {
                    @Override
                    public CompletionStage<Collection<RuleDescription>> apply(Message responseMessage) {
                        CompletableFuture<Collection<RuleDescription>> returningFuture = new CompletableFuture<>();

                        Collection<RuleDescription> rules = new ArrayList<RuleDescription>();
                        int statusCode = RequestResponseUtils.getResponseStatusCode(responseMessage);
                        if (statusCode == ClientConstants.REQUEST_RESPONSE_OK_STATUS_CODE) {
                            Map responseBodyMap = RequestResponseUtils.getResponseBody(responseMessage);
                            ArrayList<Map> rulesMap = (ArrayList<Map>) responseBodyMap.get(ClientConstants.REQUEST_RESPONSE_RULES);
                            for (Map ruleMap : rulesMap) {
                                DescribedType ruleDescription = (DescribedType) ruleMap.get("rule-description");
                                rules.add(RequestResponseUtils.decodeRuleDescriptionMap(ruleDescription));
                            }

                            TRACE_LOGGER.debug("Fetched {} rules from entity '{}'", rules.size(), MiscRequestResponseOperationHandler.this.entityPath);
                            returningFuture.complete(rules);
                        } else if (statusCode == ClientConstants.REQUEST_RESPONSE_NOCONTENT_STATUS_CODE) {
                            returningFuture.complete(rules);
                        } else {
                            // error response
                            TRACE_LOGGER.error("Fetching rules for entity '{}' failed with status code '{}'", MiscRequestResponseOperationHandler.this.entityPath, statusCode);
                            returningFuture.completeExceptionally(RequestResponseUtils.genereateExceptionFromResponse(responseMessage));
                        }

                        return returningFuture;
                    }
                }, MessagingFactory.INTERNAL_THREAD_POOL);
            }
        }, MessagingFactory.INTERNAL_THREAD_POOL);
    }
}
