/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.servicebus.primitives;

import com.microsoft.azure.servicebus.ClientSettings;
import com.microsoft.azure.servicebus.amqp.*;
import com.microsoft.azure.servicebus.security.SecurityToken;
import java8.util.concurrent.CompletableFuture;
import java8.util.concurrent.CompletionStage;
import java8.util.function.BiFunction;
import java8.util.function.Function;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.*;
import org.apache.qpid.proton.reactor.Reactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.threeten.bp.Duration;
import org.threeten.bp.Instant;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.UnresolvedAddressException;
import java.util.LinkedList;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;

/**
 * Abstracts all AMQP related details and encapsulates an AMQP connection and manages its life cycle. Each instance of this class represent one AMQP connection to the namespace.
 * If an application creates multiple senders, receivers or clients using the same MessagingFacotry instance, all those senders, receivers or clients will share the same connection to the namespace.
 *
 * @since 1.0
 */
public class MessagingFactory extends ClientEntity implements IAmqpConnection {
    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(MessagingFactory.class);
    public static final ExecutorService INTERNAL_THREAD_POOL = Executors.newCachedThreadPool();

    private static final String REACTOR_THREAD_NAME_PREFIX = "ReactorThread";
    private static final int MAX_CBS_LINK_CREATION_ATTEMPTS = 3;
    private final String hostName;
    private final CompletableFuture<Void> connetionCloseFuture;
    private final ConnectionHandler connectionHandler;
    private final ReactorHandler reactorHandler;
    private final LinkedList<Link> registeredLinks;
    private final Object reactorLock;
    private final RequestResponseLinkcache managementLinksCache;

    private Reactor reactor;
    private ReactorDispatcher reactorScheduler;
    private Connection connection;

    private CompletableFuture<MessagingFactory> factoryOpenFuture;
    private CompletableFuture<Void> cbsLinkCreationFuture;
    private RequestResponseLink cbsLink;
    private int cbsLinkCreationAttempts = 0;
    private Throwable lastCBSLinkCreationException = null;

    private final ClientSettings clientSettings;

    private MessagingFactory(URI namespaceEndpointUri, ClientSettings clientSettings) {
        super("MessagingFactory".concat(StringUtil.getShortRandomString()));
        this.clientSettings = clientSettings;

        this.hostName = namespaceEndpointUri.getHost();
        this.registeredLinks = new LinkedList<Link>();
        this.connetionCloseFuture = new CompletableFuture<Void>();
        this.reactorLock = new Object();
        this.connectionHandler = new ConnectionHandler(this);
        this.factoryOpenFuture = new CompletableFuture<MessagingFactory>();
        this.cbsLinkCreationFuture = new CompletableFuture<Void>();
        this.managementLinksCache = new RequestResponseLinkcache(this);
        this.reactorHandler = new ReactorHandler() {
            @Override
            public void onReactorInit(Event e) {
                super.onReactorInit(e);

                final Reactor r = e.getReactor();
                TRACE_LOGGER.info("Creating connection to host '{}:{}'", hostName, ClientConstants.AMQPS_PORT);
                connection = r.connectionToHost(hostName, MessagingFactory.this.connectionHandler.getPort(), connectionHandler);
            }
        };
        Timer.register(this.getClientId());
    }

    @Override
    public String getHostName() {
        return this.hostName;
    }

    private Reactor getReactor() {
        synchronized (this.reactorLock) {
            return this.reactor;
        }
    }

    private ReactorDispatcher getReactorScheduler() {
        synchronized (this.reactorLock) {
            return this.reactorScheduler;
        }
    }

    private void startReactor(ReactorHandler reactorHandler) throws IOException {
        TRACE_LOGGER.info("Creating and starting reactor");
        Reactor newReactor = ProtonUtil.reactor(reactorHandler);
        synchronized (this.reactorLock) {
            this.reactor = newReactor;
            this.reactorScheduler = new ReactorDispatcher(newReactor);
        }

        String reactorThreadName = REACTOR_THREAD_NAME_PREFIX + UUID.randomUUID().toString();
        Thread reactorThread = new Thread(new RunReactor(), reactorThreadName);
        reactorThread.start();
        TRACE_LOGGER.info("Started reactor");
    }

    Connection getActiveConnectionOrNothing() {
        if (this.connection == null || this.connection.getLocalState() == EndpointState.CLOSED || this.connection.getRemoteState() == EndpointState.CLOSED) {
            return null;
        } else {
            return this.connection;
        }
    }

    Connection getActiveConnectionCreateIfNecessary() {
        if (this.connection == null || this.connection.getLocalState() == EndpointState.CLOSED || this.connection.getRemoteState() == EndpointState.CLOSED) {
            TRACE_LOGGER.info("Creating connection to host '{}:{}'", this.hostName, ClientConstants.AMQPS_PORT);
            this.connection = this.getReactor().connectionToHost(this.hostName, ClientConstants.AMQPS_PORT, this.connectionHandler);
        }

        return this.connection;
    }

    /**
     * Gets the operation timeout from the connections string.
     *
     * @return operation timeout specified in the connection string
     */
    public Duration getOperationTimeout() {
        return this.clientSettings.getOperationTimeout();
    }

    /**
     * Gets the retry policy from the connection string.
     *
     * @return retry policy specified in the connection string
     */
    public RetryPolicy getRetryPolicy() {
        return this.clientSettings.getRetryPolicy();
    }

    public ClientSettings getClientSetttings() {
        return this.clientSettings;
    }

    public static CompletableFuture<MessagingFactory> createFromNamespaceNameAsyc(String sbNamespaceName, ClientSettings clientSettings) {
        return createFromNamespaceEndpointURIAsyc(Util.convertNamespaceToEndPointURI(sbNamespaceName), clientSettings);
    }

    public static CompletableFuture<MessagingFactory> createFromNamespaceEndpointURIAsyc(URI namespaceEndpointURI, ClientSettings clientSettings) {
        if (TRACE_LOGGER.isInfoEnabled()) {
            TRACE_LOGGER.info("Creating messaging factory from namespace endpoint uri '{}'", namespaceEndpointURI.toString());
        }

        MessagingFactory messagingFactory = new MessagingFactory(namespaceEndpointURI, clientSettings);
        try {
            messagingFactory.startReactor(messagingFactory.reactorHandler);
        } catch (IOException e) {
            Marker fatalMarker = MarkerFactory.getMarker(ClientConstants.FATAL_MARKER);
            TRACE_LOGGER.error(fatalMarker, "Starting reactor failed", e);
            messagingFactory.factoryOpenFuture.completeExceptionally(e);
        }
        return messagingFactory.factoryOpenFuture;
    }

    public static MessagingFactory createFromNamespaceName(String sbNamespaceName, ClientSettings clientSettings) throws InterruptedException, ServiceBusException {
        return completeFuture(createFromNamespaceNameAsyc(sbNamespaceName, clientSettings));
    }

    public static MessagingFactory createFromNamespaceEndpointURI(URI namespaceEndpointURI, ClientSettings clientSettings) throws InterruptedException, ServiceBusException {
        return completeFuture(createFromNamespaceEndpointURIAsyc(namespaceEndpointURI, clientSettings));
    }

    /**
     * Creates an instance of MessagingFactory from the given connection string builder. This is a non-blocking method.
     *
     * @param builder connection string builder to the  bus namespace or entity
     * @return a <code>CompletableFuture</code> which completes when a connection is established to the namespace or when a connection couldn't be established.
     * @see CompletableFuture
     */
    public static CompletableFuture<MessagingFactory> createFromConnectionStringBuilderAsync(final ConnectionStringBuilder builder) {
        if (TRACE_LOGGER.isInfoEnabled()) {
            TRACE_LOGGER.info("Creating messaging factory from connection string '{}'", builder.toLoggableString());
        }

        return createFromNamespaceEndpointURIAsyc(builder.getEndpoint(), Util.getClientSettingsFromConnectionStringBuilder(builder));
    }

    /**
     * Creates an instance of MessagingFactory from the given connection string. This is a non-blocking method.
     *
     * @param connectionString connection string to the  bus namespace or entity
     * @return a <code>CompletableFuture</code> which completes when a connection is established to the namespace or when a connection couldn't be established.
     * @see CompletableFuture
     */
    public static CompletableFuture<MessagingFactory> createFromConnectionStringAsync(final String connectionString) {
        ConnectionStringBuilder builder = new ConnectionStringBuilder(connectionString);
        return createFromConnectionStringBuilderAsync(builder);
    }

    /**
     * Creates an instance of MessagingFactory from the given connection string builder. This method blocks for a connection to the namespace to be established.
     *
     * @param builder connection string builder to the  bus namespace or entity
     * @return an instance of MessagingFactory
     * @throws InterruptedException if blocking thread is interrupted
     * @throws ExecutionException   if a connection couldn't be established to the namespace. Cause of the failure can be found by calling {@link Exception#getCause()}
     */
    public static MessagingFactory createFromConnectionStringBuilder(final ConnectionStringBuilder builder) throws InterruptedException, ExecutionException {
        return createFromConnectionStringBuilderAsync(builder).get();
    }

    /**
     * Creates an instance of MessagingFactory from the given connection string. This method blocks for a connection to the namespace to be established.
     *
     * @param connectionString connection string to the  bus namespace or entity
     * @return an instance of MessagingFactory
     * @throws InterruptedException if blocking thread is interrupted
     * @throws ExecutionException   if a connection couldn't be established to the namespace. Cause of the failure can be found by calling {@link Exception#getCause()}
     */
    public static MessagingFactory createFromConnectionString(final String connectionString) throws InterruptedException, ExecutionException {
        return createFromConnectionStringAsync(connectionString).get();
    }

    /**
     * Internal method.&nbsp;Clients should not use this method.
     */
    @Override
    public void onConnectionOpen() {
        if (!factoryOpenFuture.isDone()) {
            TRACE_LOGGER.info("MessagingFactory opened.");
            AsyncUtil.completeFuture(this.factoryOpenFuture, this);
        }

        // Connection opened. Initiate new cbs link creation
        TRACE_LOGGER.info("Connection opened to host.");
        if (this.cbsLink == null) {
            this.createCBSLinkAsync();
        }
    }

    /**
     * Internal method.&nbsp;Clients should not use this method.
     */
    @Override
    public void onConnectionError(ErrorCondition error) {
        if (error != null && error.getCondition() != null) {
            TRACE_LOGGER.error("Connection error. '{}'", error);
        }

        if (!this.factoryOpenFuture.isDone()) {
            AsyncUtil.completeFutureExceptionally(this.factoryOpenFuture, ExceptionUtil.toException(error));
            this.setClosed();
        } else {
            this.closeConnection(error, null);
        }

        if (this.getIsClosingOrClosed() && !this.connetionCloseFuture.isDone()) {
            TRACE_LOGGER.info("Connection to host closed.");
            AsyncUtil.completeFuture(this.connetionCloseFuture, null);
            Timer.unregister(this.getClientId());
        }
    }

    private void onReactorError(Exception cause) {
        if (!this.factoryOpenFuture.isDone()) {
            TRACE_LOGGER.error("Reactor error occured", cause);
            AsyncUtil.completeFutureExceptionally(this.factoryOpenFuture, cause);
            this.setClosed();
        } else {
            if (this.getIsClosingOrClosed()) {
                return;
            }

            TRACE_LOGGER.warn("Reactor error occured", cause);

            try {
                this.startReactor(this.reactorHandler);
            } catch (IOException e) {
                Marker fatalMarker = MarkerFactory.getMarker(ClientConstants.FATAL_MARKER);
                TRACE_LOGGER.error(fatalMarker, "Re-starting reactor failed with exception.", e);
                this.onReactorError(cause);
            }

            this.closeConnection(null, cause);
        }
    }

    // One of the parameters must be null
    private void closeConnection(ErrorCondition error, Exception cause) {
        // Important to copy the reference of the connection as a call to getConnection might create a new connection while we are still in this method
        Connection currentConnection = this.connection;
        if (currentConnection != null) {
            Link[] links = this.registeredLinks.toArray(new Link[0]);
            this.registeredLinks.clear();

            TRACE_LOGGER.debug("Closing all links on the connection. Number of links '{}'", links.length);
            for (Link link : links) {
                link.close();
            }

            TRACE_LOGGER.debug("Closed all links on the connection. Number of links '{}'", links.length);

            if (currentConnection.getLocalState() != EndpointState.CLOSED) {
                TRACE_LOGGER.info("Closing connection to host");
                currentConnection.close();
            }

            for (Link link : links) {
                Handler handler = BaseHandler.getHandler(link);
                if (handler != null && handler instanceof BaseLinkHandler) {
                    BaseLinkHandler linkHandler = (BaseLinkHandler) handler;
                    if (error != null) {
                        linkHandler.processOnClose(link, error);
                    } else {
                        linkHandler.processOnClose(link, cause);
                    }
                }
            }
        }
    }

    @Override
    protected CompletableFuture<Void> onClose() {
        if (!this.getIsClosed()) {
            TRACE_LOGGER.info("Closing messaging factory");
            CompletableFuture<Void> cbsLinkCloseFuture;
            if (this.cbsLink == null) {
                cbsLinkCloseFuture = CompletableFuture.completedFuture(null);
            } else {
                TRACE_LOGGER.info("Closing CBS link");
                cbsLinkCloseFuture = this.cbsLink.closeAsync();
            }

            cbsLinkCloseFuture.thenRun(new Runnable() {
                @Override
                public void run() {
                    MessagingFactory.this.managementLinksCache.freeAsync();
                }
            }).thenRun(new Runnable() {
                @Override
                public void run() {
                    if (MessagingFactory.this.cbsLinkCreationFuture != null && !MessagingFactory.this.cbsLinkCreationFuture.isDone()) {
                        MessagingFactory.this.cbsLinkCreationFuture.completeExceptionally(new Exception("Connection closed."));
                    }

                    if (MessagingFactory.this.connection != null && MessagingFactory.this.connection.getRemoteState() != EndpointState.CLOSED) {
                        try {
                            MessagingFactory.this.scheduleOnReactorThread(new DispatchHandler() {
                                @Override
                                public void onEvent() {
                                    if (MessagingFactory.this.connection != null && MessagingFactory.this.connection.getLocalState() != EndpointState.CLOSED) {
                                        TRACE_LOGGER.info("Closing connection to host");
                                        MessagingFactory.this.connection.close();
                                    }
                                }
                            });
                        } catch (IOException e) {
                            MessagingFactory.this.connetionCloseFuture.completeExceptionally(e);
                        }

                        Timer.schedule(new Runnable() {
                                           @Override
                                           public void run() {
                                               if (!MessagingFactory.this.connetionCloseFuture.isDone()) {
                                                   String errorMessage = "Closing MessagingFactory timed out.";
                                                   TRACE_LOGGER.warn(errorMessage);
                                                   AsyncUtil.completeFutureExceptionally(MessagingFactory.this.connetionCloseFuture, new TimeoutException(errorMessage));
                                               }
                                           }
                                       },
                                MessagingFactory.this.clientSettings.getOperationTimeout(), TimerType.OneTimeRun);
                    } else {
                        MessagingFactory.this.connetionCloseFuture.complete(null);
                        Timer.unregister(MessagingFactory.this.getClientId());
                    }
                }
            });

            return this.connetionCloseFuture;
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private class RunReactor implements Runnable {
        final private Reactor rctr;

        public RunReactor() {
            this.rctr = MessagingFactory.this.getReactor();
        }

        @Override
        public void run() {
            TRACE_LOGGER.info("starting reactor instance.");
            try {
                this.rctr.setTimeout(3141);
                this.rctr.start();
                boolean continuteProcessing = true;
                while (!Thread.interrupted() && continuteProcessing) {
                    // If factory is closed, stop reactor too
                    if (MessagingFactory.this.getIsClosed()) {
                        TRACE_LOGGER.info("Gracefully releasing reactor thread as messaging factory is closed");
                        break;
                    }
                    continuteProcessing = this.rctr.process();
                }
                TRACE_LOGGER.info("Stopping reactor");
                this.rctr.stop();
            } catch (HandlerException handlerException) {
                Throwable cause = handlerException.getCause();
                if (cause == null) {
                    cause = handlerException;
                }

                TRACE_LOGGER.warn("UnHandled exception while processing events in reactor:", handlerException);

                String message = !StringUtil.isNullOrEmpty(cause.getMessage()) ?
                        cause.getMessage() :
                        !StringUtil.isNullOrEmpty(handlerException.getMessage()) ?
                                handlerException.getMessage() :
                                "Reactor encountered unrecoverable error";
                ServiceBusException sbException = new ServiceBusException(
                        true,
                        String.format(Locale.US, "%s, %s", message, ExceptionUtil.getTrackingIDAndTimeToLog()),
                        cause);

                if (cause instanceof UnresolvedAddressException) {
                    sbException = new CommunicationException(
                            String.format(Locale.US, "%s. This is usually caused by incorrect hostname or network configuration. Please check to see if namespace information is correct. %s", message, ExceptionUtil.getTrackingIDAndTimeToLog()),
                            cause);
                }

                MessagingFactory.this.onReactorError(sbException);
            } finally {
                this.rctr.free();
            }
        }
    }

    /**
     * Internal method.&nbsp;Clients should not use this method.
     */
    @Override
    public void registerForConnectionError(Link link) {
        if (link != null) {
            this.registeredLinks.add(link);
        }
    }

    /**
     * Internal method.&nbsp;Clients should not use this method.
     */
    @Override
    public void deregisterForConnectionError(Link link) {
        if (link != null) {
            this.registeredLinks.remove(link);
        }
    }

    void scheduleOnReactorThread(final DispatchHandler handler) throws IOException {
        this.getReactorScheduler().invoke(handler);
    }

    void scheduleOnReactorThread(final int delay, final DispatchHandler handler) throws IOException {
        this.getReactorScheduler().invoke(delay, handler);
    }

    CompletableFuture<ScheduledFuture<?>> sendSecurityTokenAndSetRenewTimer(final String sasTokenAudienceURI, final boolean retryOnFailure, final Runnable validityRenewer) {
        final CompletableFuture<ScheduledFuture<?>> result = new CompletableFuture<ScheduledFuture<?>>();
        TRACE_LOGGER.debug("Sending token for {}", sasTokenAudienceURI);
        CompletableFuture<Instant> sendTokenFuture = this.generateAndSendSecurityToken(sasTokenAudienceURI);
        sendTokenFuture.handleAsync(new BiFunction<Instant, Throwable, Object>() {
            @Override
            public Object apply(Instant validUntil, Throwable sendTokenEx) {
                if (sendTokenEx == null) {
                    TRACE_LOGGER.debug("Sent CBS token for {} and setting renew timer", sasTokenAudienceURI);
                    ScheduledFuture<?> renewalFuture = MessagingFactory.scheduleRenewTimer(validUntil, validityRenewer);
                    result.complete(renewalFuture);
                } else {
                    Throwable sendFailureCause = ExceptionUtil.extractAsyncCompletionCause(sendTokenEx);
                    TRACE_LOGGER.warn("Sending CBS Token for {} failed.", sasTokenAudienceURI, sendFailureCause);
                    if (retryOnFailure) {
                        // Just schedule another attempt
                        TRACE_LOGGER.info("Will retry sending CBS Token for {} after {} seconds.", sasTokenAudienceURI, ClientConstants.DEFAULT_SAS_TOKEN_SEND_RETRY_INTERVAL_IN_SECONDS);
                        ScheduledFuture<?> renewalFuture = Timer.schedule(validityRenewer, Duration.ofSeconds(ClientConstants.DEFAULT_SAS_TOKEN_SEND_RETRY_INTERVAL_IN_SECONDS), TimerType.OneTimeRun);
                        result.complete(renewalFuture);
                    } else {
                        if (sendFailureCause instanceof TimeoutException) {
                            // Retry immediately on timeout. This is a special case as CBSLink may be disconnected right after the token is sent, but before it reaches the service
                            TRACE_LOGGER.debug("Resending token for {}", sasTokenAudienceURI);
                            CompletableFuture<Instant> resendTokenFuture = MessagingFactory.this.generateAndSendSecurityToken(sasTokenAudienceURI);
                            resendTokenFuture.handleAsync(new BiFunction<Instant, Throwable, Object>() {
                                @Override
                                public Object apply(Instant resendValidUntil, Throwable resendTokenEx) {
                                    if (resendTokenEx == null) {
                                        TRACE_LOGGER.debug("Sent CBS token for {} and setting renew timer", sasTokenAudienceURI);
                                        ScheduledFuture<?> renewalFuture = MessagingFactory.scheduleRenewTimer(resendValidUntil, validityRenewer);
                                        result.complete(renewalFuture);
                                    } else {
                                        Throwable resendFailureCause = ExceptionUtil.extractAsyncCompletionCause(resendTokenEx);
                                        TRACE_LOGGER.warn("Resending CBS Token for {} failed.", sasTokenAudienceURI, resendFailureCause);
                                        result.completeExceptionally(resendFailureCause);
                                    }
                                    return null;
                                }
                            }, MessagingFactory.INTERNAL_THREAD_POOL);
                        } else {
                            result.completeExceptionally(sendFailureCause);
                        }
                    }
                }
                return null;
            }
        }, MessagingFactory.INTERNAL_THREAD_POOL);

        return result;
    }

    private CompletableFuture<Instant> generateAndSendSecurityToken(String sasTokenAudienceURI) {
        CompletableFuture<SecurityToken> tokenFuture = this.clientSettings.getTokenProvider().getSecurityTokenAsync(sasTokenAudienceURI);
        return tokenFuture.thenComposeAsync(new Function<SecurityToken, CompletionStage<Instant>>() {
            @Override
            public CompletionStage<Instant> apply(SecurityToken t) {
                final SecurityToken generatedSecurityToken = t;
                return MessagingFactory.this.cbsLinkCreationFuture.thenComposeAsync(new Function<Void, CompletionStage<Instant>>() {
                    @Override
                    public CompletionStage<Instant> apply(Void v) {
                        return CommonRequestResponseOperations.sendCBSTokenAsync(MessagingFactory.this.cbsLink, ClientConstants.SAS_TOKEN_SEND_TIMEOUT, generatedSecurityToken).thenApply(new Function<Void, Instant>() {
                            @Override
                            public Instant apply(Void u) {
                                return generatedSecurityToken.getValidUntil();
                            }
                        });
                    }
                }, MessagingFactory.INTERNAL_THREAD_POOL);
            }
        }, MessagingFactory.INTERNAL_THREAD_POOL);
    }

    private static ScheduledFuture<?> scheduleRenewTimer(Instant currentTokenValidUntil, Runnable validityRenewer) {
        if (currentTokenValidUntil == Instant.MAX) {
            // User provided token or will never expire
            return null;
        } else {
            // It will eventually expire. Renew it
            int renewInterval = Util.getTokenRenewIntervalInSeconds((int) Duration.between(Instant.now(), currentTokenValidUntil).getSeconds());
            return Timer.schedule(validityRenewer, Duration.ofSeconds(renewInterval), TimerType.OneTimeRun);
        }
    }

    CompletableFuture<RequestResponseLink> obtainRequestResponseLinkAsync(String entityPath, MessagingEntityType entityType) {
        this.throwIfClosed(null);
        return this.managementLinksCache.obtainRequestResponseLinkAsync(entityPath, entityType);
    }

    void releaseRequestResponseLink(String entityPath) {
        if (!this.getIsClosed()) {
            this.managementLinksCache.releaseRequestResponseLink(entityPath);
        }
    }

    private void createCBSLinkAsync() {
        if (this.getIsClosingOrClosed()) {
            return;
        }

        if (++this.cbsLinkCreationAttempts > MAX_CBS_LINK_CREATION_ATTEMPTS) {
            Throwable completionEx = this.lastCBSLinkCreationException == null ? new Exception("CBS link creation failed multiple times.") : this.lastCBSLinkCreationException;
            this.cbsLinkCreationFuture.completeExceptionally(completionEx);
        } else {
            final String requestResponseLinkPath = RequestResponseLink.getCBSNodeLinkPath();
            TRACE_LOGGER.info("Creating CBS link to {}", requestResponseLinkPath);
            RequestResponseLink.createAsync(this, this.getClientId() + "-cbs", requestResponseLinkPath, null, null).handleAsync(new BiFunction<RequestResponseLink, Throwable, Object>() {
                @Override
                public Object apply(RequestResponseLink cbsLink, Throwable ex) {
                    if (ex == null) {
                        TRACE_LOGGER.info("Created CBS link to {}", requestResponseLinkPath);
                        if (MessagingFactory.this.getIsClosingOrClosed()) {
                            // Factory is closed before CBSLink could be created. Close the created CBS link too
                            cbsLink.closeAsync();
                        } else {
                            MessagingFactory.this.cbsLink = cbsLink;
                            MessagingFactory.this.cbsLinkCreationFuture.complete(null);
                        }
                    } else {
                        MessagingFactory.this.lastCBSLinkCreationException = ExceptionUtil.extractAsyncCompletionCause(ex);
                        TRACE_LOGGER.warn("Creating CBS link to {} failed. Attempts '{}'", requestResponseLinkPath, MessagingFactory.this.cbsLinkCreationAttempts);
                        MessagingFactory.this.createCBSLinkAsync();
                    }
                    return null;
                }
            }, MessagingFactory.INTERNAL_THREAD_POOL);

        }
    }

    private static <T> T completeFuture(CompletableFuture<T> future) throws InterruptedException, ServiceBusException {
        try {
            return future.get();
        } catch (InterruptedException ie) {
            // Rare instance
            throw ie;
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof ServiceBusException) {
                throw (ServiceBusException) cause;
            } else {
                throw new ServiceBusException(true, cause);
            }
        }
    }
}
