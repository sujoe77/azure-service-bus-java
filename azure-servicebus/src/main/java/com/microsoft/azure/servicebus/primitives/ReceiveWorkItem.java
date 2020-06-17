package com.microsoft.azure.servicebus.primitives;

import org.threeten.bp.Duration;
import java.util.Collection;
import java8.util.concurrent.CompletableFuture;

class ReceiveWorkItem extends WorkItem<Collection<MessageWithDeliveryTag>>
{
	private final int maxMessageCount;

	public ReceiveWorkItem(CompletableFuture<Collection<MessageWithDeliveryTag>> completableFuture, Duration timeout, final int maxMessageCount)
	{
		super(completableFuture, timeout);
		this.maxMessageCount = maxMessageCount;
	}
	
	public int getMaxMessageCount()
	{
		return this.maxMessageCount;
	}
}
