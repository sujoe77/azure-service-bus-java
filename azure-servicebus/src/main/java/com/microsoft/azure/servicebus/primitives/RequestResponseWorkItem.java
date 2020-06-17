package com.microsoft.azure.servicebus.primitives;

import org.threeten.bp.Duration;
import java8.util.concurrent.CompletableFuture;

import org.apache.qpid.proton.message.Message;

public class RequestResponseWorkItem extends WorkItem<Message>
{
	Message request;
	
	public RequestResponseWorkItem(Message request, CompletableFuture<Message> completableFuture, TimeoutTracker tracker) {
		super(completableFuture, tracker);
		this.request = request;
	}
	
	public RequestResponseWorkItem(Message request, CompletableFuture<Message> completableFuture, Duration timeout) {
		super(completableFuture, timeout);
		this.request = request;
	}
	
	public Message getRequest()
	{
		return this.request;
	}
}
