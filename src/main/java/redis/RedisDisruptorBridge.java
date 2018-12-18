package redis;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import disruptor.RedisEvent;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.codec.redis.RedisMessage;

public class RedisDisruptorBridge {
    public Bootstrap b;
    public EventLoopGroup group;
    public Channel channel;

    public RedisDisruptorBridge(Disruptor<RedisEvent> disruptor) throws InterruptedException {
        group = new NioEventLoopGroup(1);

        b = new Bootstrap();
        b.group(group).channel(NioSocketChannel.class)//
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel ch) {
                        final ChannelPipeline pipeline = ch.pipeline();
//						pipeline.addLast(new LoggingHandler(LogLevel.INFO));
                        pipeline.addLast(new RedisEncoder());
                        pipeline.addLast(new RedisDecoder());
                        pipeline.addLast(new RedisResponseHandler(disruptor));
                    }
                });

        final ChannelFuture sync = b.connect("127.0.0.1", 6379).sync();
        channel = sync.channel();
    }

    public void close() {
        group.shutdownGracefully();
    }

    public static class RedisResponseHandler extends SimpleChannelInboundHandler<RedisMessage> {
        private RingBuffer<RedisEvent> disruptor;

        public RedisResponseHandler(Disruptor<RedisEvent> disruptor) {
            this.disruptor = disruptor.getRingBuffer();
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final RedisMessage msg) {
            disruptor.publishEvent((event, sequence, redisMessage) -> event.set(redisMessage.toString()), msg);
        }
    }
}
