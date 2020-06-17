package com.microsoft.azure.servicebus;

import org.threeten.bp.Instant;

public abstract class TestSessionHandler implements ISessionHandler
{
	@Override
	public void notifyException(Throwable exception, ExceptionPhase phase) {
		System.out.println(phase + "-" + exception.getMessage()  + ":" + Instant.now());
	}
}
