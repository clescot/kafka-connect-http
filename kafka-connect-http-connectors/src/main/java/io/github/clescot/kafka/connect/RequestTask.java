package io.github.clescot.kafka.connect;

import io.github.clescot.kafka.connect.http.core.Request;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

public interface RequestTask<C extends Client,F extends Configuration<C,R>,R extends Request,E> extends Task<C,F,R>{


    CompletableFuture<E> call(@NotNull R request);
}
