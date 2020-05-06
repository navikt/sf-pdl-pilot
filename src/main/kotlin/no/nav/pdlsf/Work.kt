package no.nav.pdlsf

import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.UnstableDefault
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer

private val log = KotlinLogging.logger {}

internal data class PersonProtoObject(
    val key: PersonProto.PersonKey,
    val value: PersonProto.PersonValue
)

@OptIn(UnstableDefault::class)
@ImplicitReflectionSerializer
internal fun work(params: Params) {
    log.info { "bootstrap work session starting" }

    val cache = createCache(params)
    if (!ServerState.isOk() && cache.isEmpty()) {
        log.error { "Terminating work session since cache is empty due to kafka issues" }
        return
    }

    log.info { "Get kafkaproducer to send protobuf person objects to SF topic" }
    getKafkaProducerByConfig<ByteArray, ByteArray>(
            mapOf(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to params.envVar.kBrokers,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                    ProducerConfig.ACKS_CONFIG to "all",
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to params.envVar.kProducerTimeout,
                    ProducerConfig.CLIENT_ID_CONFIG to params.envVar.kClientID
            ).let { map ->
                if (params.envVar.kSecurityEnabled)
                    map.addKafkaSecurity(params.vault.kafkaUser, params.vault.kafkaPassword, params.envVar.kSecProt, params.envVar.kSaslMec)
                else map
            }
    ) {

        log.info { "Start building up map of person from PDL compaction log" }
        val km: MutableMap<ByteArray, ByteArray?> = mutableMapOf()
        val pilotFnrList = params.vault.pilotList
        log.info { "string - $pilotFnrList" }
        log.info { "list - ${pilotFnrList.reader().readLines()}}" }

        val results = pilotFnrList.reader().readLines().map { fnr ->
            //
            //
            // getPersonFromGraphQL(fnr)
            Pair<ConsumerStates, PersonBase>(ConsumerStates.IsOk, PersonUnknown)
        }
        val areOk = results.fold(true) { acc, resp -> acc && (resp.first == ConsumerStates.IsOk) }

        val finalRes = if (areOk) {
            log.info { "${results.size} consumer records resulted in number of Person ${results.filter { it.second is Person }.count()}, Tombestone ${results.filter { it.second is PersonTombestone }.count()}, Dead  ${results.filter { it.second is PersonDead }.count()}, Unknown ${results.filter { it.second is PersonUnknown }.count()}" }
            results.forEach { pair ->
                val personBase = pair.second
                if (personBase is PersonTombestone) {
                    val personTombstoneProtoKey = personBase.toPersonTombstoneProtoKey()
                    km[personTombstoneProtoKey.toByteArray()] = null
                } else if (personBase is Person) {
                    val personProto = personBase.toPersonProto()
                    val status = cache.exists(personBase.aktoerId, personProto.second.hashCode())
                    Metrics.publishedPersons.labels(status.name).inc()
                    if (status in listOf(ObjectInCacheStatus.New, ObjectInCacheStatus.Updated)) {
                        km[personProto.first.toByteArray()] = personProto.second.toByteArray()
                    }
                }
            }
            Pair(ConsumerStates.IsOk, km)
        } else {
            Pair(ConsumerStates.HasIssues, km)
        }

        if (finalRes.first == ConsumerStates.IsOk) {
                    log.info { "${finalRes.second.size} - protobuf Person objects sent to topic ${params.envVar.kTopicSf}" }
            finalRes.second.forEach { m ->
                        this.send(ProducerRecord(params.envVar.kTopicSf, m.key, m.value))
                    }
                    ConsumerStates.IsOk
                } else {
                    log.error { "Consumerstate issues, is not Ok." }
                    ConsumerStates.HasIssues
                }
    }
    log.info { "Start polling sf topic for  new or updated records" }
    getSalesforceSObjectPostFun(params.getSalesforceDetails()) { sfPost ->
        getKafkaConsumerByConfig<ByteArray, ByteArray>(
                mapOf(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to params.envVar.kBrokers,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                        ConsumerConfig.GROUP_ID_CONFIG to params.envVar.kClientID,
                        ConsumerConfig.CLIENT_ID_CONFIG to params.envVar.kClientID,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
                ).let { cMap ->
                    if (params.envVar.kSecurityEnabled)
                        cMap.addKafkaSecurity(params.vault.kafkaUser, params.vault.kafkaPassword, params.envVar.kSecProt, params.envVar.kSaslMec)
                    else cMap
                },
                listOf(params.envVar.kTopicSf), fromBeginning = false
        ) { cRecords ->
            log.info { "${cRecords.count()} - new or updated records ready to process to SF" }

            if (!cRecords.isEmpty) {
                // TODO :: SF postering
                val list = cRecords.map { PersonProtoObject(it.key().protobufSafeParseKey(), it.value().protobufSafeParseValue()) }
                log.info { "${list.toJsonPayload(Params().envVar)}" }
//                ConsumerStates.IsOkNoCommit

                when (sfPost(cRecords
                        .map { PersonProtoObject(it.key().protobufSafeParseKey(), it.value().protobufSafeParseValue()) }
                        )) {
                    true -> ConsumerStates.IsOk
                    false -> ConsumerStates.HasIssues
                }
                ConsumerStates.IsOkNoCommit
            } else {
                log.info { "Kafka events completed for now - leaving kafka consumer loop" }
                ConsumerStates.IsFinished
            }
        }
    }
}
