package no.nav.pdlsf

import java.io.Serializable
import java.time.Duration
import java.util.Properties
import kotlin.Exception
import mu.KotlinLogging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SaslConfigs

private val log = KotlinLogging.logger {}

internal sealed class ConsumerStates {
    object IsOk : ConsumerStates()
    object IsOkNoCommit : ConsumerStates()
    object HasIssues : ConsumerStates()
    object IsFinished : ConsumerStates()
}

internal fun <K, V> getKafkaProducerByConfig(config: Map<String, Any>, doProduce: KafkaProducer<K, V>.() -> Unit): Boolean =
        try {
            KafkaProducer<K, V>(
                    Properties().apply { config.forEach { set(it.key, it.value) } }
            ).use {
                it.doProduce()
                log.info { "KafkaProducer closing" }
            }
            true
        } catch (e: Exception) {
            ServerState.flag(ServerStates.KafkaIssues)
            log.error { "KafkaProducer failure during construction - ${e.message}" }
            false
        }

internal fun <K, V> getKafkaConsumerByConfig(
    config: Map<String, Any>,
    topics: List<String>,
    fromBeginning: Boolean = false,
    doConsume: (ConsumerRecords<K, V>) -> ConsumerStates
): Boolean =
        try {
            KafkaConsumer<K, V>(Properties().apply { config.forEach { set(it.key, it.value) } })
                    .apply {
                        if (fromBeginning)
                            this.runCatching {
                                assign(
                                        topics.flatMap { topic ->
                                            partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
                                        }
                                )
                            }.onFailure {
                                ServerState.flag(ServerStates.KafkaIssues)
                                log.error { "Failure for topic partition(s) assignment for $topics - ${it.message}" }
                            }
                        else
                            this.runCatching {
                                subscribe(topics)
                            }.onFailure {
                                ServerState.flag(ServerStates.KafkaIssues)
                                log.error { "Failure during subscription for $topics -  ${it.message}" }
                            }
                    }
                    .use { c ->

                        if (fromBeginning) c.runCatching {
                            c.seekToBeginning(emptyList())
                        }.onFailure {
                            ServerState.flag(ServerStates.KafkaIssues)
                            log.error { "Failure for SeekToBeginning - ${it.message}" }
                        }

                        tailrec fun loop(keepGoing: Boolean): Unit = when {
                            ShutdownHook.isActive() || !ServerState.isOk() || !keepGoing -> Unit
                            else -> loop(c.pollAndConsumptionIsOk(doConsume))
                        }

                        loop(true)

                        log.info { "Closing KafkaConsumer" }
                    }
            true
        } catch (e: Exception) {
            ServerState.flag(ServerStates.KafkaIssues)
            log.error { "Failure during kafka consumer construction - ${e.message}" }
            false
        }

private fun <K, V> KafkaConsumer<K, V>.pollAndConsumptionIsOk(doConsume: (ConsumerRecords<K, V>) -> ConsumerStates): Boolean =
        runCatching { poll(Duration.ofMillis(5_000)) as ConsumerRecords<K, V> }
                .onFailure { log.error { "Failure during poll - ${it.localizedMessage}" } }
                .getOrDefault(ConsumerRecords<K, V>(emptyMap()))
                .let { cRecords ->
                    runCatching { doConsume(cRecords) }
                            .onFailure { log.error { "Failure during doConsume - ${it.localizedMessage}" } }
                            .getOrDefault(ConsumerStates.HasIssues)
                            .let { consumerState ->
                                when (consumerState) {
                                    ConsumerStates.IsOk -> {
                                        try {
                                            commitSync()
                                            true
                                        } catch (e: Exception) {
                                            log.error { "Failure during commit, leaving - ${e.message}" }
                                            false
                                        }
                                    }
                                    ConsumerStates.IsOkNoCommit -> true
                                    ConsumerStates.HasIssues -> {
                                        ServerState.flag(ServerStates.KafkaConsumerIssues)
                                        false
                                    }
                                    ConsumerStates.IsFinished -> {
                                        log.info { "Consumer logic requests stop of consumption" }
                                        false
                                    }
                                }
                            }
                }

internal fun Map<String, Serializable>.addKafkaSecurity(
    username: String,
    password: String,
    secProtocol: String = "SASL_PLAINTEXT",
    saslMechanism: String = "PLAIN"
): Map<String, Any> = this.let {

    val mMap = this.toMutableMap()

    mMap[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = secProtocol
    mMap[SaslConfigs.SASL_MECHANISM] = saslMechanism

    val jaasPainLogin = "org.apache.kafka.common.security.plain.PlainLoginModule"
    val jaasRequired = "required"

    mMap[SaslConfigs.SASL_JAAS_CONFIG] = "$jaasPainLogin $jaasRequired " +
            "username=\"$username\" password=\"$password\";"

    mMap.toMap()
}
