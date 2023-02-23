package com.silvio.log.cloud;

import java.util.List;

public interface HandleEvent {
    List<ObjectIdentifier> handleEvent(String jsonEvent);
}
