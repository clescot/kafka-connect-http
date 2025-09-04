package io.github.clescot.kafka.connect;

import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;

public interface RequestResponseClient<NR,NS> extends RequestClient<HttpRequest,NR>,ResponseClient<HttpResponse,NS> {

}
