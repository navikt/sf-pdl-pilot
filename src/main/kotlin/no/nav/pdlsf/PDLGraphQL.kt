package no.nav.pdlsf

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.KSerializer
import kotlinx.serialization.PrimitiveDescriptor
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.Serializable
import kotlinx.serialization.UnstableDefault
import kotlinx.serialization.stringify
import mu.KotlinLogging
import org.http4k.core.Method
import org.http4k.core.Status

private val log = KotlinLogging.logger { }
private const val GRAPHQL_QUERY = "/graphql/query.graphql"

@OptIn(UnstableDefault::class)
@UnstableDefault
@ImplicitReflectionSerializer
private fun executeGraphQlQuery(
    query: String,
    variables: Map<String, String>
): QueryResponseBase = Http.client.invokeWM(
        org.http4k.core.Request(Method.POST, Params().envVar.pdlGraphQlUrl)
                .header("x-nav-apiKey", Params().envVar.pdlGraphQlApiKey)
                .header("Tema", "GEN")
                .header("Authorization", "Bearer ${(getStsToken() as StsAccessToken).accessToken}")
                .header("Nav-Consumer-Token", "Bearer ${(getStsToken() as StsAccessToken).accessToken}")
                .header("Cache-Control", "no-cache")
                .header("Content-Type", "application/json")
                .body(json.stringify(QueryRequest(
                        query = query,
                        variables = variables
                )))
).let { response ->
    when (response.status) {
        Status.OK -> {
            val result = response.bodyString().getQueryResponseFromJsonString()
            result
        }
        else -> {
            log.error { "PDL GraphQl request failed - ${response.toMessage()}" }
            Metrics.failedRequestGraphQl.inc()
            InvalidQueryResponse
        }
    }
}

@ImplicitReflectionSerializer
fun getPersonFromGraphQL(ident: String): Pair<Boolean, PersonBase> {
    val query = getStringFromResource(GRAPHQL_QUERY).trim()
    var isOk: Boolean = false

    return when (val response = executeGraphQlQuery(query, mapOf("ident" to ident))) {
        is QueryErrorResponse -> {
            if (response.errors.first().mapToHttpCode().code == 404) {
                log.warn { "GraphQL aktørId $ident ikke funnet." }
                Metrics.parsedGrapQLPersons.labels(PersonUnknown.toMetricsLable()).inc()
                isOk = false
                Pair(isOk, PersonUnknown)
            } else {
                log.error { "GraphQL aktørId $ident  feilet - ${response.errors.first().message}" }
                Metrics.parsedGrapQLPersons.labels(PersonError.toMetricsLable()).inc()
                isOk = false
                Pair(isOk, PersonError)
            }
        }
        is InvalidQueryResponse -> {
            log.error { "Unable to parse graphql query response on aktørId - $ident " }
            Metrics.parsedGrapQLPersons.labels(PersonInvalid.toMetricsLable()).inc()
            isOk = false
            Pair(isOk, PersonInvalid)
        }
        is QueryResponse -> {
            val person = response.toPerson()
            if (person is PersonInvalid) {
                log.error { "Unable to parse person from qraphql response on aktørId - $ident " }
                Metrics.parsedGrapQLPersons.labels(person.toMetricsLable()).inc()
                Pair(isOk, person)
            } else {
                Metrics.parsedGrapQLPersons.labels(person.toMetricsLable()).inc()
                Pair(isOk, person)
            }
        }
    }
}

object IsoLocalDateSerializer : LocalDateSerializer(DateTimeFormatter.ISO_LOCAL_DATE)

open class LocalDateSerializer(private val formatter: DateTimeFormatter) : KSerializer<LocalDate> {
    override val descriptor: SerialDescriptor = PrimitiveDescriptor("LocalDate", PrimitiveKind.STRING) // StringDescriptor.withName("java.time.LocalDate")
    override fun deserialize(decoder: Decoder): LocalDate {
        return LocalDate.parse(decoder.decodeString(), formatter)
    }

    override fun serialize(encoder: Encoder, obj: LocalDate) {
        encoder.encodeString(obj.format(formatter))
    }
}

object IsoLocalDateTimeSerializer : LocalDateTimeSerializer(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

open class LocalDateTimeSerializer(private val formatter: DateTimeFormatter) : KSerializer<LocalDateTime> {
    override val descriptor: SerialDescriptor = PrimitiveDescriptor("LocalDateTime", PrimitiveKind.STRING) // StringDescriptor.withName("java.time.LocalDateTime")
    override fun deserialize(decoder: Decoder): LocalDateTime {
        return LocalDateTime.parse(decoder.decodeString(), formatter)
    }

    override fun serialize(encoder: Encoder, obj: LocalDateTime) {
        encoder.encodeString(obj.format(formatter))
    }
}

sealed class QueryResponseBase
object InvalidQueryResponse : QueryResponseBase()

@Serializable
data class QueryResponse(
    val data: Data,
    val errors: List<Error>? = emptyList()
) : QueryResponseBase() {
    @Serializable
    data class Data(
        val hentPerson: HentPerson,
        val hentIdenter: HentIdenter
    ) {
        @Serializable
        data class HentPerson(
            val adressebeskyttelse: List<Adressebeskyttelse>,
            val bostedsadresse: List<Bostedsadresse>,
            val doedsfall: List<Doedsfall>,
            val navn: List<Navn>,
            val sikkerhetstiltak: List<Sikkerhetstiltak>
        ) {
            @Serializable
            data class Adressebeskyttelse(
                val gradering: Gradering
            )

            @Serializable
            data class Bostedsadresse(
                val vegadresse: Vegadresse?,
                val matrikkeladresse: Matrikkeladresse?,
                val ukjentBosted: UkjentBosted?
            ) {
                @Serializable
                data class Vegadresse(
                    val kommunenummer: String
                )
                @Serializable
                data class Matrikkeladresse(
                    val kommunenummer: String
                )
                @Serializable
                data class UkjentBosted(
                    val bostedskommune: String
                )
            }
            @Serializable
            data class Doedsfall(
                @Serializable(with = IsoLocalDateSerializer::class)
                val doedsdato: LocalDate? = null
            )
            @Serializable
            data class Navn(
                val fornavn: String,
                val mellomnavn: String?,
                val etternavn: String,
                val metadata: Metadata
            ) {
                @Serializable
                data class Metadata(
                    val master: String
                )
            }
            @Serializable
            data class Sikkerhetstiltak(
                val beskrivelse: String
            )
        }
        @Serializable
        data class HentIdenter(
            val identer: List<Identer>
        ) {
            @Serializable
            data class Identer(
                val ident: String,
                val gruppe: IdentGruppe
            ) {
                @Serializable
                enum class IdentGruppe {
                    AKTORID, FOLKEREGISTERIDENT, NPID
                }
            }
        }
    }

    @Serializable
    data class Error(
        val extensions: Extensions,
        val locations: List<Location>,
        val path: List<String>,
        val message: String
    ) {
        @Serializable
        data class Extensions(
            val classification: String,
            val code: String?
        )
        @Serializable
        data class Location(
            val column: Int,
            val line: Int
        )
    }
}

@Serializable
data class QueryErrorResponse(
    val errors: List<QueryResponse.Error>
) : QueryResponseBase()

fun QueryResponse.Error.mapToHttpCode(): Status = when (this.extensions.code) {
    "unauthenticated" -> Status.FORBIDDEN
    "unauthorized" -> Status.UNAUTHORIZED
    "not_found" -> Status.NOT_FOUND
    "bad_request" -> Status.BAD_REQUEST
    "server_error" -> Status.INTERNAL_SERVER_ERROR
    else -> Status.INTERNAL_SERVER_ERROR
}
@Serializable
data class QueryRequest(
    val query: String,
    val variables: Map<String, String>,
    val operationName: String? = null
) {
    data class Variables(
        val variables: Map<String, Any>
    )
}

enum class Gradering {
    STRENGT_FORTROLIG_UTLAND,
    STRENGT_FORTROLIG,
    FORTROLIG,
    UGRADERT
}
internal const val UKJENT_FRA_PDL = "<UKJENT_FRA_PDL>"

sealed class NavnBase {
    abstract val fornavn: String
    abstract val mellomnavn: String
    abstract val etternavn: String

    data class Freg(
        override val fornavn: String,
        override val mellomnavn: String,
        override val etternavn: String
    ) : NavnBase()

    data class Pdl(
        override val fornavn: String,
        override val mellomnavn: String,
        override val etternavn: String
    ) : NavnBase()

    data class Ukjent(
        override val fornavn: String = UKJENT_FRA_PDL,
        override val mellomnavn: String = UKJENT_FRA_PDL,
        override val etternavn: String = UKJENT_FRA_PDL
    ) : NavnBase()
}

private fun QueryResponse.Data.HentPerson.findNavn(): NavnBase {
    return if (this.navn.isNullOrEmpty()) {
        NavnBase.Ukjent()
    } else {
        this.navn.firstOrNull { it.metadata.master.toUpperCase() == "FREG" }?.let {
            if (it.etternavn.isBlank() || it.fornavn.isBlank())
                NavnBase.Freg(
                        fornavn = it.fornavn,
                        etternavn = it.etternavn,
                        mellomnavn = it.mellomnavn.orEmpty()
                )
            else
                NavnBase.Ukjent(
                        fornavn = it.fornavn,
                        etternavn = it.etternavn,
                        mellomnavn = it.mellomnavn.orEmpty()
                )
        } ?: this.navn.firstOrNull { it.metadata.master.toUpperCase() == "PDL" }?.let {
            if (it.etternavn.isBlank() || it.fornavn.isBlank())
                NavnBase.Pdl(
                        fornavn = it.fornavn,
                        etternavn = it.etternavn,
                        mellomnavn = it.mellomnavn.orEmpty()
                )
            else
                NavnBase.Ukjent(
                        fornavn = it.fornavn,
                        etternavn = it.etternavn,
                        mellomnavn = it.mellomnavn.orEmpty()
                )
        } ?: NavnBase.Ukjent()
    }
}

@UnstableDefault
@ImplicitReflectionSerializer
fun String.getQueryResponseFromJsonString(): QueryResponseBase = runCatching {
        json.parse(QueryResponse.serializer(), this)
    }
        .onFailure { log.error { "Failed serialize GraphQL QueryResponse - ${it.localizedMessage}" } }
        .getOrElse {
        runCatching {
            json.parse(QueryErrorResponse.serializer(), this)
        }
                .onFailure { log.error { "Failed serialize GraphQL QueryErrorResponse - ${it.localizedMessage}" } }
                .getOrDefault(InvalidQueryResponse)
    }

fun QueryResponse.toPerson(): PersonBase {
    return runCatching { Person(
            aktoerId = this.data.hentIdenter.findAktoerId(),
            identifikasjonsnummer = this.data.hentIdenter.findFolkeregisterIdent(),
            fornavn = this.data.hentPerson.findNavn().fornavn,
            mellomnavn = this.data.hentPerson.findNavn().mellomnavn,
            etternavn = this.data.hentPerson.findNavn().etternavn,
            adressebeskyttelse = this.data.hentPerson.findAdressebeskyttelse(),
            sikkerhetstiltak = this.data.hentPerson.sikkerhetstiltak.map { it.beskrivelse }.toList(),
            kommunenummer = this.data.hentPerson.findKommunenummer(),
            region = this.data.hentPerson.findRegion(),
            doed = this.data.hentPerson.doedsfall.isNotEmpty() // "doedsdato": null  betyr at han faktsik er død, man vet bare ikke når. Listen kan ha to innslagt, kilde FREG og PDL
        )
    }
            .onFailure { log.error { "Error creating Person from a graphQL query response ${it.localizedMessage}" } }
            .getOrDefault(PersonInvalid)
}

private fun QueryResponse.Data.HentIdenter.findAktoerId(): String {
    return this.identer.let {
        if (it.isEmpty()) {
            UKJENT_FRA_PDL
        } else {
            identer.firstOrNull { it.gruppe == QueryResponse.Data.HentIdenter.Identer.IdentGruppe.AKTORID }?.ident ?: UKJENT_FRA_PDL }
        }
    }

private fun QueryResponse.Data.HentIdenter.findFolkeregisterIdent(): String {
    return this.identer.let {
        if (it.isEmpty()) {
            UKJENT_FRA_PDL
        } else {
            identer.firstOrNull { it.gruppe == QueryResponse.Data.HentIdenter.Identer.IdentGruppe.FOLKEREGISTERIDENT }?.ident ?: UKJENT_FRA_PDL }
    }
}
private fun QueryResponse.Data.HentPerson.findAdressebeskyttelse(): Gradering {
    return this.adressebeskyttelse.let {
        if (it.isEmpty()) {
            Gradering.UGRADERT
        } else {
            Gradering.valueOf(it.first().gradering.name)
        }
    }
}

fun QueryResponse.Data.HentPerson.findKommunenummer(): String {
    return this.bostedsadresse.let { bostedsadresse ->
        if (bostedsadresse.isNullOrEmpty()) {
            Metrics.usedAdresseTypes.labels(AdresseType.INGEN.name).inc()
            UKJENT_FRA_PDL
        } else {
            bostedsadresse.first().let {
                it.vegadresse?.let { vegadresse ->
                    Metrics.usedAdresseTypes.labels(AdresseType.VEGADRESSE.name).inc()
                    vegadresse.kommunenummer
                } ?: it.matrikkeladresse?.let { matrikkeladresse ->
                    Metrics.usedAdresseTypes.labels(AdresseType.MATRIKKELADRESSE.name).inc()
                    matrikkeladresse.kommunenummer
                } ?: it.ukjentBosted?.let { ukjentBosted ->
                    Metrics.usedAdresseTypes.labels(AdresseType.UKJENTBOSTED.name).inc()
                    ukjentBosted.bostedskommune
                } ?: UKJENT_FRA_PDL.also {
                    Metrics.usedAdresseTypes.labels(AdresseType.INGEN.name).inc()
                }
            }
        }
    }
}

fun QueryResponse.Data.HentPerson.findRegion(): String {
    return this.findKommunenummer().let { kommunenummer ->
        if (kommunenummer == UKJENT_FRA_PDL) kommunenummer else kommunenummer.substring(0, 2) }
    }
