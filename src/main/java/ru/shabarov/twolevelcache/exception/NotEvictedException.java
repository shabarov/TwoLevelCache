package ru.shabarov.twolevelcache.exception;

public class NotEvictedException extends RuntimeException{
    public NotEvictedException(String s) {
        super(s);
    }
}
