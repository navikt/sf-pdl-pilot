package no.nav.pdlsf

import java.io.File
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.UnstableDefault
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer

private val log = KotlinLogging.logger {}

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
                    ProducerConfig.CLIENT_ID_CONFIG to params.envVar.kClientID + "-producer"
            ).let { map ->
                if (params.envVar.kSecurityEnabled)
                    map.addKafkaSecurity(params.vault.kafkaUser, params.vault.kafkaPassword, params.envVar.kSecProt, params.envVar.kSaslMec)
                else map
            }
    ) {

        log.info { "Start building up map of person from PDL compaction log" }
        val km: MutableMap<ByteArray, ByteArray?> = mutableMapOf()
        val pilotFnrList = params.vault.pilotList
        if (pilotFnrList.isEmpty()) {
            log.error { "Pilot fnr list is empty - terminate" }
            return@getKafkaProducerByConfig
        }

        val results = pilotFnrList.reader().readLines().map { fnr ->
//            if (params.envVar.sfInstanceType == SalesforceInstancetype.SCRATCH.name) {
//                log.info { "Is SCRATCH - getPersonFromGraphQL $fnr" }
                    getPersonFromGraphQL(fnr)
//            } else {
//            Pair<ConsumerStates, PersonBase>(ConsumerStates.IsOk, Person(
//                    aktoerId = "aktørId2",
//                    identifikasjonsnummer = "fnr",
//                    fornavn = "fornavn",
//                    mellomnavn = "",
//                    etternavn = "etternavn",
//                    adressebeskyttelse = Gradering.UGRADERT,
//                    sikkerhetstiltak = listOf("sikkerhet", "sikkerhet2"),
//                    kommunenummer = "3001",
//                    region = "01",
//                    doed = false
//            ))
//            }
        }
        results.firstOrNull { it.second is Person && (it.second as Person).identifikasjonsnummer == "19118549425" }?.let {
            log.info { "Found subject store message" }
            File("/tmp/investigate").writeText("Findings:\n+${(it.second as Person).toJson()}")
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
                    if (status in listOf(ObjectInCacheStatus.New, ObjectInCacheStatus.Updated, ObjectInCacheStatus.NoChange)) {
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
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                        ConsumerConfig.GROUP_ID_CONFIG to params.envVar.kClientID + "-consumer",
                        ConsumerConfig.CLIENT_ID_CONFIG to params.envVar.kClientID + "-consumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 200, // Use of SF REST API
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
                val body = SFsObjectRest(
                        records = cRecords.map {
                            val key = it.key().protobufSafeParseKey()
                            val value = it.value().protobufSafeParseValue()
                            val person = Person(
                                    aktoerId = key.aktoerId,
                                    identifikasjonsnummer = value.identifikasjonsnummer,
                                    fornavn = value.fornavn,
                                    mellomnavn = value.mellomnavn,
                                    etternavn = value.etternavn,
                                    adressebeskyttelse = Gradering.valueOf(value.adressebeskyttelse.name),
                                    sikkerhetstiltak = value.sikkerhetstiltakList,
                                    kommunenummer = value.kommunenummer,
                                    region = value.region,
                                    doed = value.doed
                            ).toJson()
                            KafkaMessage(
                                    topic = it.topic(),
                                    key = (key.aktoerId),
                                    value = person.encodeB64()
                            )
                        }
                ).toJson()
                if (sfPost(body)) {
                    Metrics.sentLayOff.inc(cRecords.count().toDouble())
                    log.info { "Post of ${cRecords.count()} layoff(s) to Salesforce" }

                    // reset alarm metric
                    if (ServerState.isOk()) Metrics.failedRequest.clear()

                    ConsumerStates.IsOk
                } else {
                    log.error { "Couldn't post ${cRecords.count()} layoff(s) to Salesforce - leaving kafka consumer loop" }
                    ConsumerStates.HasIssues
                }
            } else {
                log.info { "Kafka events completed for now - leaving kafka consumer loop" }
                ConsumerStates.IsFinished
            }
        }
    }
}
