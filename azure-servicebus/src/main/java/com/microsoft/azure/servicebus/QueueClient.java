// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.azure.servicebus;

import com.microsoft.azure.servicebus.primitives.*;
import java8.util.concurrent.CompletableFuture;
import java8.util.concurrent.CompletionStage;
import java8.util.function.BiFunction;
import java8.util.function.Consumer;
import java8.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Instant;

import java.net.URI;
import java.sql.Date;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

public final class QueueClient extends InitializableEntity implements IQueueClient {
    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(QueueClient.class);
    private final ReceiveMode receiveMode;
    private final String queuePath;
    private final Object senderCreationLock;
    private MessagingFactory factory;
    private IMessageSender sender;
    private CompletableFuture<Void> senderCreationFuture;

    private MessageAndSessionPump messageAndSessionPump;
    private SessionBrowser sessionBrowser;
    private MiscRequestResponseOperationHandler miscRequestResponseHandler;

    private QueueClient(ReceiveMode receiveMode, String queuePath) {
        super(StringUtil.getShortRandomString());
        this.receiveMode = receiveMode;
        this.queuePath = queuePath;
        this.senderCreationLock = new Object();
    }

    public QueueClient(final ConnectionStringBuilder amqpConnectionStringBuilder, final ReceiveMode receiveMode) throws InterruptedException, ServiceBusException {
        this(receiveMode, amqpConnectionStringBuilder.getEntityPath());
        CompletableFuture<MessagingFactory> factoryFuture = MessagingFactory.createFromConnectionStringBuilderAsync(amqpConnectionStringBuilder);
        Utils.completeFuture(factoryFuture.thenComposeAsync(new Function<MessagingFactory, CompletableFuture<Void>>() {
            @Override
            public CompletableFuture<Void> apply(MessagingFactory f) {
                return QueueClient.this.createInternals(f, amqpConnectionStringBuilder.getEntityPath(), receiveMode);
            }
        }, MessagingFactory.INTERNAL_THREAD_POOL));
        if (TRACE_LOGGER.isInfoEnabled()) {
            TRACE_LOGGER.info("Created queue client to connection string '{}'", amqpConnectionStringBuilder.toLoggableString());
        }
    }

    public QueueClient(String namespace, String queuePath, ClientSettings clientSettings, ReceiveMode receiveMode) throws InterruptedException, ServiceBusException {
        this(Util.convertNamespaceToEndPointURI(namespace), queuePath, clientSettings, receiveMode);
    }

    public QueueClient(URI namespaceEndpointURI, final String queuePath, ClientSettings clientSettings, final ReceiveMode receiveMode) throws InterruptedException, ServiceBusException {
        this(receiveMode, queuePath);
        CompletableFuture<MessagingFactory> factoryFuture = MessagingFactory.createFromNamespaceEndpointURIAsyc(namespaceEndpointURI, clientSettings);
        Utils.completeFuture(factoryFuture.thenComposeAsync(new Function<MessagingFactory, CompletableFuture<Void>>() {
            @Override
            public CompletableFuture<Void> apply(MessagingFactory f) {
                return QueueClient.this.createInternals(f, queuePath, receiveMode);
            }
        }, MessagingFactory.INTERNAL_THREAD_POOL));
        if (TRACE_LOGGER.isInfoEnabled()) {
            TRACE_LOGGER.info("Created queue client to queue '{}/{}'", namespaceEndpointURI.toString(), queuePath);
        }
    }

    QueueClient(MessagingFactory factory, String queuePath, ReceiveMode receiveMode) throws InterruptedException, ServiceBusException {
        this(receiveMode, queuePath);
        Utils.completeFuture(this.createInternals(factory, queuePath, receiveMode));
        if (TRACE_LOGGER.isInfoEnabled()) {
            TRACE_LOGGER.info("Created queue client to queue '{}'", queuePath);
        }
    }

    private CompletableFuture<Void> createInternals(final MessagingFactory factory, final String queuePath, ReceiveMode receiveMode) {
        this.factory = factory;

        CompletableFuture<Void> postSessionBrowserFuture = MiscRequestResponseOperationHandler.create(factory, queuePath, MessagingEntityType.QUEUE).thenAcceptAsync(new Consumer<MiscRequestResponseOperationHandler>() {
            @Override
            public void accept(MiscRequestResponseOperationHandler msoh) {
                QueueClient.this.miscRequestResponseHandler = msoh;
                QueueClient.this.sessionBrowser = new SessionBrowser(factory, queuePath, MessagingEntityType.QUEUE, msoh);
            }
        }, MessagingFactory.INTERNAL_THREAD_POOL);

        this.messageAndSessionPump = new MessageAndSessionPump(factory, queuePath, MessagingEntityType.QUEUE, receiveMode);
        CompletableFuture<Void> messagePumpInitFuture = this.messageAndSessionPump.initializeAsync();

        return CompletableFuture.allOf(postSessionBrowserFuture, messagePumpInitFuture);
    }

    private CompletableFuture<Void> createSenderAsync() {
        synchronized (this.senderCreationLock) {
            if (this.senderCreationFuture == null) {
                this.senderCreationFuture = new CompletableFuture<Void>();
                ClientFactory.createMessageSenderFromEntityPathAsync(this.factory, this.queuePath, MessagingEntityType.QUEUE).handleAsync(new BiFunction<IMessageSender, Throwable, Object>() {
                    @Override
                    public Object apply(IMessageSender sender, Throwable ex) {
                        if (ex == null) {
                            QueueClient.this.sender = sender;
                            QueueClient.this.senderCreationFuture.complete(null);
                        } else {
                            Throwable cause = ExceptionUtil.extractAsyncCompletionCause(ex);
                            QueueClient.this.senderCreationFuture.completeExceptionally(cause);
                            // Set it to null so next call will retry sender creation
                            synchronized (QueueClient.this.senderCreationLock) {
                                QueueClient.this.senderCreationFuture = null;
                            }
                        }
                        return null;
                    }
                }, MessagingFactory.INTERNAL_THREAD_POOL);
            }

            return this.senderCreationFuture;
        }
    }

    private CompletableFuture<Void> closeSenderAsync() {
        synchronized (this.senderCreationLock) {
            if (this.senderCreationFuture != null) {
                CompletableFuture<Void> senderCloseFuture = this.senderCreationFuture.thenComposeAsync(new Function<Void, CompletionStage<Void>>() {
                    @Override
                    public CompletionStage<Void> apply(Void v) {
                        return QueueClient.this.sender.closeAsync();
                    }
                }, MessagingFactory.INTERNAL_THREAD_POOL);
                this.senderCreationFuture = null;
                return senderCloseFuture;
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    @Override
    public ReceiveMode getReceiveMode() {
        return this.receiveMode;
    }

    @Override
    public void send(IMessage message) throws InterruptedException, ServiceBusException {
        Utils.completeFuture(this.sendAsync(message));
    }

    @Override
    public void sendBatch(Collection<? extends IMessage> messages) throws InterruptedException, ServiceBusException {
        Utils.completeFuture(this.sendBatchAsync(messages));
    }

    @Override
    public CompletableFuture<Void> sendAsync(final IMessage message) {
        return this.createSenderAsync().thenComposeAsync(new Function<Void, CompletionStage<Void>>() {
            @Override
            public CompletionStage<Void> apply(Void v) {
                return QueueClient.this.sender.sendAsync(message);
            }
        }, MessagingFactory.INTERNAL_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> sendBatchAsync(final Collection<? extends IMessage> messages) {
        return this.createSenderAsync().thenComposeAsync(new Function<Void, CompletionStage<Void>>() {
            @Override
            public CompletionStage<Void> apply(Void v) {
                return QueueClient.this.sender.sendBatchAsync(messages);
            }
        }, MessagingFactory.INTERNAL_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Long> scheduleMessageAsync(final IMessage message, final Instant scheduledEnqueueTimeUtc) {
        return this.createSenderAsync().thenComposeAsync(new Function<Void, CompletionStage<Long>>() {
            @Override
            public CompletionStage<Long> apply(Void v) {
                return QueueClient.this.sender.scheduleMessageAsync(message, scheduledEnqueueTimeUtc);
            }
        }, MessagingFactory.INTERNAL_THREAD_POOL);
    }

    @Override
    public CompletableFuture<Void> cancelScheduledMessageAsync(final long sequenceNumber) {
        return this.createSenderAsync().thenComposeAsync(new Function<Void, CompletionStage<Void>>() {
            @Override
            public CompletionStage<Void> apply(Void v) {
                return QueueClient.this.sender.cancelScheduledMessageAsync(sequenceNumber);
            }
        }, MessagingFactory.INTERNAL_THREAD_POOL);
    }

    @Override
    public long scheduleMessage(IMessage message, Instant scheduledEnqueueTimeUtc) throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(this.scheduleMessageAsync(message, scheduledEnqueueTimeUtc));
    }

    @Override
    public void cancelScheduledMessage(long sequenceNumber) throws InterruptedException, ServiceBusException {
        Utils.completeFuture(this.cancelScheduledMessageAsync(sequenceNumber));
    }

    @Override
    public String getEntityPath() {
        return this.queuePath;
    }

    @Deprecated
    @Override
    public void registerMessageHandler(IMessageHandler handler) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.registerMessageHandler(handler);
    }

    @Deprecated
    @Override
    public void registerMessageHandler(IMessageHandler handler, MessageHandlerOptions handlerOptions) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.registerMessageHandler(handler, handlerOptions);
    }

    @Deprecated
    @Override
    public void registerSessionHandler(ISessionHandler handler) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.registerSessionHandler(handler);
    }

    @Deprecated
    @Override
    public void registerSessionHandler(ISessionHandler handler, SessionHandlerOptions handlerOptions) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.registerSessionHandler(handler, handlerOptions);
    }

    @Override
    public void registerMessageHandler(IMessageHandler handler, ExecutorService executorService) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.registerMessageHandler(handler, executorService);
    }

    @Override
    public void registerMessageHandler(IMessageHandler handler, MessageHandlerOptions handlerOptions, ExecutorService executorService) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.registerMessageHandler(handler, handlerOptions, executorService);
    }

    @Override
    public void registerSessionHandler(ISessionHandler handler, ExecutorService executorService) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.registerSessionHandler(handler, executorService);
    }

    @Override
    public void registerSessionHandler(ISessionHandler handler, SessionHandlerOptions handlerOptions, ExecutorService executorService) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.registerSessionHandler(handler, handlerOptions, executorService);
    }

    // No op now
    @Override
    CompletableFuture<Void> initializeAsync() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<Void> onClose() {
        return this.messageAndSessionPump.closeAsync().thenCompose(new Function<Void, CompletionStage<Void>>() {
            @Override
            public CompletionStage<Void> apply(Void v) {
                return QueueClient.this.closeSenderAsync().thenCompose(new Function<Void, CompletableFuture<Void>>() {
                    @Override
                    public CompletableFuture<Void> apply(Void u) {
                        return QueueClient.this.miscRequestResponseHandler.closeAsync().thenCompose(new Function<Void, CompletableFuture<Void>>() {
                            @Override
                            public CompletableFuture<Void> apply(Void w) {
                                return QueueClient.this.factory.closeAsync();
                            }
                        });
                    }
                });
            }
        });
    }

    //	@Override
    Collection<IMessageSession> getMessageSessions() throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(this.getMessageSessionsAsync());
    }

    //	@Override
    Collection<IMessageSession> getMessageSessions(Instant lastUpdatedTime) throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(this.getMessageSessionsAsync(lastUpdatedTime));
    }

    //	@Override
    CompletableFuture<Collection<IMessageSession>> getMessageSessionsAsync() {
        return this.sessionBrowser.getMessageSessionsAsync();
    }

    //	@Override
    CompletableFuture<Collection<IMessageSession>> getMessageSessionsAsync(Instant lastUpdatedTime) {
        return this.sessionBrowser.getMessageSessionsAsync(new Date(lastUpdatedTime.toEpochMilli()));
    }

    @Override
    public void abandon(UUID lockToken) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.abandon(lockToken);
    }

    @Override
    public void abandon(UUID lockToken, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.abandon(lockToken, propertiesToModify);
    }

    @Override
    public CompletableFuture<Void> abandonAsync(UUID lockToken) {
        return this.messageAndSessionPump.abandonAsync(lockToken);
    }

    @Override
    public CompletableFuture<Void> abandonAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
        return this.messageAndSessionPump.abandonAsync(lockToken, propertiesToModify);
    }

    @Override
    public void complete(UUID lockToken) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.complete(lockToken);
    }

    @Override
    public CompletableFuture<Void> completeAsync(UUID lockToken) {
        return this.messageAndSessionPump.completeAsync(lockToken);
    }

    //	@Override
    void defer(UUID lockToken) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.defer(lockToken);
    }

    //	@Override
    void defer(UUID lockToken, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.defer(lockToken, propertiesToModify);
    }

//    @Override
//    public CompletableFuture<Void> deferAsync(UUID lockToken) {
//        return this.messageAndSessionPump.deferAsync(lockToken);
//    }
//
//    @Override
//    public CompletableFuture<Void> deferAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
//        return this.messageAndSessionPump.deferAsync(lockToken, propertiesToModify);
//    }

    @Override
    public void deadLetter(UUID lockToken) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.deadLetter(lockToken);
    }

    @Override
    public void deadLetter(UUID lockToken, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.deadLetter(lockToken, propertiesToModify);
    }

    @Override
    public void deadLetter(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.deadLetter(lockToken, deadLetterReason, deadLetterErrorDescription);
    }

    @Override
    public void deadLetter(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
        this.messageAndSessionPump.deadLetter(lockToken, deadLetterReason, deadLetterErrorDescription, propertiesToModify);
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken) {
        return this.messageAndSessionPump.deadLetterAsync(lockToken);
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
        return this.messageAndSessionPump.deadLetterAsync(lockToken, propertiesToModify);
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription) {
        return this.messageAndSessionPump.deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription);
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription, Map<String, Object> propertiesToModify) {
        return this.messageAndSessionPump.deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription, propertiesToModify);
    }

    @Override
    public int getPrefetchCount() {
        return this.messageAndSessionPump.getPrefetchCount();
    }

    @Override
    public void setPrefetchCount(int prefetchCount) throws ServiceBusException {
        this.messageAndSessionPump.setPrefetchCount(prefetchCount);
    }

    @Override
    public String getQueueName() {
        return this.getEntityPath();
    }
}
