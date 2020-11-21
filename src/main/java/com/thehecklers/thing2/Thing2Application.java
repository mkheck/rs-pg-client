package com.thehecklers.thing2;

import io.rsocket.SocketAcceptor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.UUID;

@SpringBootApplication
public class Thing2Application {

    public static void main(String[] args) {
        SpringApplication.run(Thing2Application.class, args);
    }

    @Bean
    RSocketRequester requester(RSocketRequester.Builder builder, RSocketStrategies strategies) {
        //return builder.tcp("localhost", 8090);

        // Replace above single line with what follows to enable
        // server-initiated request
        RSocketRequester requester;
        String clientId = UUID.randomUUID().toString();

        SocketAcceptor responder = RSocketMessageHandler.responder(strategies, new ClientHandler());

        requester = builder
                .setupRoute("thing2")
                .setupData(clientId)
                .rsocketStrategies(strategies)
                .rsocketConnector(connector -> connector.acceptor(responder))
                .tcp("localhost", 8090);

/*
        // Problematic, errors out now
        requester.rsocket()
                .onClose()
                .doOnError(err -> System.out.println("Connection CLOSED"))
                .doFinally(con -> System.out.println("Client DISCONNECTED"))
                .subscribe();
*/

        return requester;
    }
}

@Component
class Something {
    private final RSocketRequester requester;

    public Something(RSocketRequester requester) {
        this.requester = requester;
    }

    void reqResp() {
        requester.route("reqresp")
                .data(new Ping("This is my Ping"))
                .retrieveMono(Pong.class)
                .log()
                .block();
    }

    void reqStreamImperativePing() {
        requester.route("reqstream")
                .data(new Ping("Ping for Pong stream"))
                .retrieveFlux(Pong.class)
                .log()
                .subscribe();
    }

    void reqStreamReactivePing() {
        requester.route("reactivereqstream")
                .data(Mono.just(new Ping("Ping for Pong stream")))
                .retrieveFlux(Pong.class)
                .log()
                .subscribe();
    }

    void fireAndForget() {
        requester.route("fireforget")
                .data(Mono.just(new Ping("Inbound!")))
                .send()
                .subscribe();
    }

    //    @PostConstruct
    void bidirectionalStream() {
        requester.route("bidirectional")
                .data(Flux.interval(Duration.ofSeconds(1))
                        //.map(l -> new Ping("Inbound Ping " + l.toString())))
                        .map(l -> new Ping(" <<< Ping from Thing 2: " + l.toString())))
                .retrieveFlux(Pong.class)
                //.log()
                .subscribe();
    }

    @PreDestroy
    void disposeRSocketConnection() {
        if (null != requester.rsocket()) {
            requester.rsocket().dispose();
        }
    }
}

class ClientHandler {
    @MessageMapping("client")
    Flux<Ping> handleRequest(Mono<Pong> pongMono) {
        return pongMono.log().thenMany(
                Flux.interval(Duration.ofSeconds(1))
                .map(l -> new Ping(" <<< Ping from Thing 2: " + l.toString())));
    }
}

@Component
class SomethingElse {
    private final RSocketRequester requester;

    public SomethingElse(RSocketRequester requester) {
        this.requester = requester;
    }

    @PostConstruct
    void reqStreamReactive() {
        requester.route("reactivereqstream")
                .data(Mono.just(new Ping("Ping for Pong stream")))
                .retrieveFlux(Pong.class)
                .log()
                .subscribe();
    }
}