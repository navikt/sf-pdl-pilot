package no.nav.pdlsf

import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.ImplicitReflectionSerializer
import mu.KotlinLogging

@ImplicitReflectionSerializer
object Bootstrap {


    private val log = KotlinLogging.logger { }

    fun start(ev: EnvVar = EnvVar()) {
        log.info { "Starting" }
        ShutdownHook.reset()
        enableNAISAPI {
            ServerState.reset()
            loop(ev)
        }
        log.info { "Finished!" }
    }

    private tailrec fun loop(ev: EnvVar) {

        log.info { "Get parameters from dynamic vault and static env. variables" }
        val p = Params(
                vault = Vault(),
                envVar = ev
        )

        when (val i = p.integrityCheck()) {
            is IntegrityIssue -> {
                log.error { i.cause }
                ServerState.flag(ServerStates.IntegrityIssues)
                // flag need for attention
                // TODO - fix misuse of metric giving alert on slack
                Metrics.failedRequest.inc()
            }
            is IntegrityOk -> {
                log.info { "Proxy details: ${p.envVar.httpsProxy}" }

                // some resets before next attempt/work session
                Metrics.sessionReset()

                work(p) // ServerState will be updated according to any issues

                if (!ShutdownHook.isActive() && ServerState.isOk()) conditionalWait(p.envVar.msBetweenWork)

                if (!ShutdownHook.isActive() && ServerState.isOk()) loop(ev)
            }
        }
    }

    private fun conditionalWait(ms: Long) = runBlocking {

        log.info { "Will wait $ms ms before starting all over" }

        val cr = launch {
            runCatching { delay(ms) }
                    .onSuccess { log.info { "waiting completed" } }
                    .onFailure { log.info { "waiting interrupted" } }
        }

        tailrec suspend fun loop(): Unit = when {
            cr.isCompleted -> Unit
            ServerState.preStopIsActive() || ShutdownHook.isActive() -> cr.cancel()
            else -> {
                delay(250L)
                loop()
            }
        }

        loop()
        cr.join()
    }
}
