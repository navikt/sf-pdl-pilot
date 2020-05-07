package no.nav.pdlsf

import java.net.URI
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import org.apache.http.HttpHost
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.HttpClients
import org.http4k.client.ApacheClient
import org.http4k.core.HttpHandler
import org.http4k.core.Request
import org.http4k.core.Response

internal val json = Json(
        JsonConfiguration(
                ignoreUnknownKeys = true,
                isLenient = true
        )
)

object Http {
    val client: HttpHandler by lazy { ApacheClient.proxy() }
    val clientWithoutProxy: HttpHandler by lazy { ApacheClient.invoke() }
}

fun ApacheClient.proxy(): HttpHandler = EnvVar().httpsProxy.let { p ->

    when {
        p.isEmpty() -> this()
        // TODO:: må håndtere proxy for salesforce og ikke for STS og GraphQL
        else -> {
            val up = URI(p)
            this(client =
            HttpClients.custom()
                    .setDefaultRequestConfig(
                            RequestConfig.custom()
                                    .setProxy(HttpHost(up.host, up.port, up.scheme))
                                    .setRedirectsEnabled(false)
                                    .setCookieSpec(CookieSpecs.IGNORE_COOKIES)
                                    .build())
                    .build()
            )
        }
    }
}

fun HttpHandler.invokeWM(r: Request): Response = Metrics.responseLatency.labels(r.uri.toString()).startTimer().let { rt ->
    this.invoke(r).also { rt.observeDuration() }
}
