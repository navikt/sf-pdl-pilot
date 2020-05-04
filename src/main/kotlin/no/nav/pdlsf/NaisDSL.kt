package no.nav.pdlsf

import io.prometheus.client.exporter.common.TextFormat
import java.io.IOException
import java.io.StringWriter
import mu.KotlinLogging
import no.nav.pdlsf.Metrics.cRegistry
import org.http4k.core.Method
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.routing.bind
import org.http4k.routing.routes
import org.http4k.server.Netty
import org.http4k.server.asServer

private val log = KotlinLogging.logger { }

const val NAIS_URL = "http://localhost:8080"

const val NAIS_ISALIVE = "/isAlive"
const val NAIS_ISREADY = "/isReady"
const val NAIS_METRICS = "/metrics"
const val NAIS_PRESTOP = "/stop"

private val api = routes(

        NAIS_ISALIVE bind Method.GET to { Response(Status.OK).body("is alive") },
        NAIS_ISREADY bind Method.GET to { Response(Status.OK).body("is ready") },
        NAIS_METRICS bind Method.GET to {
            val content = try {
                StringWriter().let { str ->
                    TextFormat.write004(str, cRegistry.metricFamilySamples())
                    str
                }.toString()
            } catch (e: IOException) {
                log.error { "/prometheus failed writing metrics - ${e.message}" }
                ""
            }
            if (content.isNotEmpty()) Response(Status.OK).body(content)
            else Response(Status.NO_CONTENT).body(content)
        },
        NAIS_PRESTOP bind Method.GET to {
            Metrics.preStopHook.inc()
            ServerState.flag(ServerStates.PreStopHookActive)
            log.info { "Received PreStopHook from NAIS" }
            Response(Status.OK).body("")
        }
)

fun enableNAISAPI(doSomething: () -> Unit) {
    val srv = api.asServer(Netty(8080))

    try {
        srv.start()
        log.info { "NAIS DSL is up and running" }
        doSomething()
        srv.stop()
    } catch (e: Exception) {
        log.error { "Could not enable/disable NAIS api for port 8080" }
    } finally {
        srv.close()
        log.info { "NAIS DSL is stopped" }
    }
}
