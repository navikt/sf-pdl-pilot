package no.nav.pdlsf

import com.google.protobuf.InvalidProtocolBufferException
import kotlinx.serialization.json.JsonElement
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto.PersonKey
import no.nav.pdlsf.proto.PersonProto.PersonValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer

private val log = KotlinLogging.logger { }

fun createCache(params: Params): Map<String, Int?> {
    val cache: MutableMap<String, Int?> = mutableMapOf()

    getKafkaConsumerByConfig<ByteArray, ByteArray>(
            mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to params.envVar.kBrokers,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                    ConsumerConfig.GROUP_ID_CONFIG to params.envVar.kClientID + "-cache",
                    ConsumerConfig.CLIENT_ID_CONFIG to params.envVar.kClientID + "-cache",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
            ).let { cMap ->
                if (params.envVar.kSecurityEnabled)
                    cMap.addKafkaSecurity(params.vault.kafkaUser, params.vault.kafkaPassword, params.envVar.kSecProt, params.envVar.kSaslMec)
                else cMap
            },
            listOf(params.envVar.kTopicSf), fromBeginning = true
    ) { cRecords ->
        if (!cRecords.isEmpty) {
            cRecords.forEach { record ->
                val aktoerId = record.key().protobufSafeParseKey().aktoerId
                if (record.value() == null) {
                    cache[aktoerId] = null
                } else {
                    cache[aktoerId] = record.value().protobufSafeParseValue().hashCode()
                }
            }
            ConsumerStates.IsOkNoCommit
        } else {
            log.info { "Kafka events completed for now creating cache - leaving kafka consumer loop" }
            ConsumerStates.IsFinished
        }
    }
    Metrics.cachedPersons.inc(cache.size.toDouble())
    log.info { "Finished building up Cache of compaction log size person ${cache.size}" }

    return cache
}

fun JsonElement.isAlive(): Boolean = runCatching {
    jsonObject.content["hentPerson"]?.let { hi ->
        hi.jsonObject.content["doedsfall"]?.let {
            it.jsonArray.isNullOrEmpty()
        } ?: true
    } ?: true
}
.onFailure { log.info { "Failure resolving if person is Alive - ${it.localizedMessage}" } }
.getOrDefault(true)

sealed class PersonBase

object PersonUnknown : PersonBase() // Http 200, men person finnes ikke. Tibakemelding i Errors
object PersonInvalid : PersonBase() // Konvertering fra response graphQl til Person mislykkes
object PersonError : PersonBase() // Internal error/network/unauthorized
object PersonDead : PersonBase() // Dødperson

data class PersonTombestone(
    val aktoerId: String
) : PersonBase() {
    fun toPersonTombstoneProtoKey(): PersonKey =
            PersonKey.newBuilder().apply {
                aktoerId = this@PersonTombestone.aktoerId
            }.build()
}

data class Person(
    val aktoerId: String = "",
    val identifikasjonsnummer: String = "",
    val fornavn: String = "",
    val mellomnavn: String = "",
    val etternavn: String = "",
    val adressebeskyttelse: Gradering = Gradering.UGRADERT,
    val sikkerhetstiltak: List<String> = emptyList(),
    val kommunenummer: String = "",
    val region: String = "",
    val doed: Boolean = false
) : PersonBase() {

    fun toPersonProto(): Pair<PersonKey, PersonValue> =
            PersonKey.newBuilder().apply {
                aktoerId = this@Person.aktoerId
            }.build() to PersonValue.newBuilder().apply {
                identifikasjonsnummer = this@Person.identifikasjonsnummer
                fornavn = this@Person.fornavn
                mellomnavn = this@Person.mellomnavn
                etternavn = this@Person.etternavn
                adressebeskyttelse = PersonValue.Gradering.valueOf(this@Person.adressebeskyttelse.name)
                this@Person.sikkerhetstiltak.forEach {
                    addSikkerhetstiltak(it)
                }
                kommunenummer = this@Person.kommunenummer
                region = this@Person.region
                doed = this@Person.doed
            }
                    .build()
}

fun PersonBase.toMetricsLable(): String {
    return when (this) {
        is PersonDead -> "DEAD"
        is PersonUnknown -> "UNKNOWN"
        is PersonTombestone -> "TOMBESTONE"
        is PersonInvalid -> "INVALID"
        is PersonError -> "ERROR"
        is Person -> "VALID"
    }
}

internal fun ByteArray.protobufSafeParseKey(): PersonKey = this.let { ba ->
    try {
        PersonKey.parseFrom(ba)
    } catch (e: InvalidProtocolBufferException) {
        PersonKey.getDefaultInstance()
    }
}

internal fun ByteArray.protobufSafeParseValue(): PersonValue = this.let { ba ->
    try {
        PersonValue.parseFrom(ba)
    } catch (e: InvalidProtocolBufferException) {
        PersonValue.getDefaultInstance() // TODO:: Mulig dette trefer tombestone
    }
}

internal sealed class ObjectInCacheStatus(val name: String) {
    object New : ObjectInCacheStatus("NY")
    object Updated : ObjectInCacheStatus("ENDRET")
    object NoChange : ObjectInCacheStatus("UENDRET")
}

internal fun Map<String, Int?>.exists(aktoerId: String, newValueHash: Int): ObjectInCacheStatus =
        if (!this.containsKey(aktoerId))
            ObjectInCacheStatus.New
        else if ((this.containsKey(aktoerId) && this[aktoerId] != newValueHash))
            ObjectInCacheStatus.Updated
        else
            ObjectInCacheStatus.NoChange
