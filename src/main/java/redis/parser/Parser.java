package redis.parser;

import io.netty.handler.codec.redis.RedisMessage;

public interface Parser {

    /**
     * Method responsible for handling response received from Netty's Redis coded.
     *
     * @param msg
     * @return return true if there is no more response's parts expected and current response state can be cleared.
     */
    boolean parse(final RedisMessage msg);
}
