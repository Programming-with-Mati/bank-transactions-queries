package com.github.programmingwithmati.repository.exceptions;

public class ObjectNotFoundException extends RuntimeException {

    public ObjectNotFoundException(Object key, String storeName) {
        super("Object not found in store %s for key %s".formatted(storeName, key.toString()));
    }
}
