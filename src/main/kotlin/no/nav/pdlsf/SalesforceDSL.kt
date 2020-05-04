package no.nav.pdlsf

import com.fasterxml.jackson.databind.JsonNode
import java.security.KeyStore
import java.security.PrivateKey
import java.util.Base64
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Status
import org.http4k.format.Jackson

private val log = KotlinLogging.logger { }

data class Salesforce(
    val instancetype: SalesforceInstancetype,
    val url: String,
    val version: String,
    val clientID: String,
    val username: String,
    val keystore: KeystoreBase
) {
    fun configIsOk(): Boolean = clientID.isNotEmpty() && username.isNotEmpty()

    private fun getJWTClaimset() = JWTClaimSet(
            iss = clientID,
            aud = url,
            sub = username,
            exp = ((System.currentTimeMillis() / 1000) + 300).toString()
    )

    fun authorize(): AuthorizationBase = getJWTClaimset().let { cs ->

        when (keystore) {
            is KeystoreMissing -> AuthorizationMissing
            is KeyStoreDetails ->
                when (val signature = keystore.sign(getHeaderClaimset(cs).toByteArray(), oAuthEndpoint())) {
                    is MissingSignature -> AuthorizationMissing
                    is Signature -> signature.getAuthByJWT(cs)
                }
        }
    }

    fun oAuthEndpoint() = "$url/services/oauth2/token"
    fun sObjectPath() = "/services/data/$version/composite/sobjects"
}

enum class SalesforceInstancetype { MOCK, SCRATCH, PREPROD, PRODUCTION }

sealed class KeystoreBase {

    companion object {
        fun getPrivatekey(ksB64: String, ksPwd: String, pkAlias: String, pkPwd: String): KeystoreBase = runCatching {
            KeyStoreDetails(
                KeyStore.getInstance("JKS")
                        .apply { load(ksB64.decodeB64().inputStream(), ksPwd.toCharArray()) }
                        .run { getKey(pkAlias, pkPwd.toCharArray()) as PrivateKey }
            )
        }
                .onFailure {
                    ServerState.flag(ServerStates.KeystoreIssues)
                    log.error { "Keystore issues - ${it.localizedMessage}" }
                }
                .getOrDefault(KeystoreMissing)
    }

    fun signCheckIsOk(): Boolean = when (this) {
        is KeystoreMissing -> false
        is KeyStoreDetails -> when (this.sign("something".toByteArray(), "")) {
            is MissingSignature -> false
            is Signature -> true
        }
    }
}
object KeystoreMissing : KeystoreBase()
data class KeyStoreDetails(
    val privateKey: PrivateKey
) : KeystoreBase() {
    fun sign(data: ByteArray, oAuthEndpoint: String): SignatureBase = runCatching {
        java.security.Signature.getInstance("SHA256withRSA")
                .apply {
                    initSign(privateKey)
                    update(data)
                }
                .run { Signature(sign().encodeB64(), oAuthEndpoint) }
    }
            .onFailure {
                ServerState.flag(ServerStates.KeystoreIssues)
                log.error { "Signing data failed - ${it.localizedMessage}" }
            }
            .getOrDefault(MissingSignature)
}

@Serializable
data class JWTHeader(val alg: String = "RS256") {
    fun toJson(): String = json.stringify(serializer(), this)
}

sealed class JWTClaimSetBase
object JWTClaimSetMissing : JWTClaimSetBase()

@Serializable
data class JWTClaimSet(
    val iss: String,
    val aud: String,
    val sub: String,
    val exp: String
) : JWTClaimSetBase() {

    companion object {
        // use in MOCK utilities
        fun fromJson(data: String): JWTClaimSetBase = runCatching { json.parse(serializer(), data) }
                .onFailure {
                    log.error { "Parsing of authorization response failed - ${it.localizedMessage}" }
                }
                .getOrDefault(JWTClaimSetMissing)
    }

    fun toJson(): String = json.stringify(serializer(), this)
}

internal fun getHeaderClaimset(cs: JWTClaimSet): String =
        "${JWTHeader().toJson().encodeB64()}.${cs.toJson().encodeB64()}"

fun ByteArray.encodeB64(): String = org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(this)
fun String.encodeB64(): String = org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(this.toByteArray())
fun String.decodeB64(): ByteArray = org.apache.commons.codec.binary.Base64.decodeBase64(this)

sealed class SignatureBase
object MissingSignature : SignatureBase()
data class Signature(val content: String, val oAuthEndpoint: String) : SignatureBase() {

    fun getAuthByJWT(cs: JWTClaimSet): AuthorizationBase = Http.client.invokeWM(
            Request(Method.POST, oAuthEndpoint)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .query("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
                    .query("assertion", "${getHeaderClaimset(cs)}.$content")
                    .body("")
    ).let { response ->
        when (response.status) {
            Status.OK -> {
                Metrics.successfulRequest.inc()
                Authorization.fromJson(response.bodyString())
            }
            else -> {
                Metrics.failedRequest.inc()
                ServerState.flag(ServerStates.SalesforceIssues)
                log.error { "Authorization request failed - ${response.status.description}(${response.status.code})" }
                AuthorizationMissing
            }
        }
    }
}

sealed class AuthorizationBase
object AuthorizationMissing : AuthorizationBase()

@Serializable
data class Authorization(
    val access_token: String = "",
    val scope: String = "",
    val instance_url: String = "",
    val id: String = "",
    val token_type: String = "",
    val issued_at: String = "",
    val signature: String = ""
) : AuthorizationBase() {
    companion object {
        fun fromJson(data: String): AuthorizationBase = runCatching { json.parse(serializer(), data) }
                .onFailure {
                    ServerState.flag(ServerStates.SalesforceIssues)
                    log.error { "Parsing of authorization response failed - ${it.localizedMessage}" }
                }
                .getOrDefault(AuthorizationMissing)
    }

    fun getPostRequest(sObjectPath: String): Request = Request(
            Method.POST, "$instance_url$sObjectPath")
            .header("Authorization", "$token_type $access_token")
            .header("Content-Type", "application/json;charset=UTF-8")
}

internal fun getSalesforceSObjectPostFun(
    sf: Salesforce,
    doSomething: ((List<PersonProtoObject>) -> Boolean) -> Unit
): Boolean = sf.authorize().let { authorization ->

    fun failure(error: String): Boolean {
        Metrics.failedRequest.inc()
        ServerState.flag(ServerStates.SalesforceIssues)
        log.error { error }
        return false
    }

    tailrec fun doPost(list: List<PersonProtoObject>, a: AuthorizationBase, retries: Int = 1): Boolean = when (a) {
        is AuthorizationMissing -> false
        is Authorization -> {
            val r = Http.client.invokeWM(a.getPostRequest(sf.sObjectPath()).body(list.toJsonPayload(Params().envVar)))
            when (r.status) {
                Status.OK -> {
                    SFsObjectStatus.fromJson(r.bodyString()).let { status ->
                        when (status.isInvalid()) {
                            true -> failure("Post response error - SFsObjectStatusInvalid")
                            else -> {
                                if (status.filterIsInstance<SFsObjectStatus>().all { it.success }) {
                                    Metrics.successfulRequest.inc()
                                    true
                                } else failure("Post error - ${status.filterIsInstance<SFsObjectStatus>().getErrMessage()}")
                            }
                        }
                    }
                }
                Status.UNAUTHORIZED -> {
                    if (retries <= 3) {
                        log.info { "${r.status.description} - refresh of token, attempt no: $retries" }
                        Metrics.failedRequest.inc()
                        runCatching { runBlocking { delay((retries * 1_000).toLong()) } }
                        doPost(list, sf.authorize(), retries + 1)
                    } else failure("Failed to reauthorize with Salesforce")
                }
                else -> {
                    failure("Post error - ${r.status.description}")
                }
            }
        }
    }

    fun doSomethingPost(b: List<PersonProtoObject>): Boolean = doPost(b, authorization)

    when (authorization) {
        is AuthorizationMissing -> false
        is Authorization -> {
            doSomething(::doSomethingPost)
            true
        }
    }
}

private fun List<PersonProtoObject>.toJsonPayload(ev: EnvVar): String = Jackson.let { json ->

    json.obj(
            "allOrNone" to json.boolean(true),
            "records" to json.array(this.map { it.toJson(ev) })
    ).toString()
}

private fun PersonProtoObject.toJson(ev: EnvVar): JsonNode = Jackson.let { json ->

    json.obj(
            "attributes" to json.obj("type" to json.string("KafkaMessage__c")),
            "CRM_Topic__c" to json.string(ev.kTopicSf),
            "CRM_Key__c" to json.string("${this.key.aktoerId }"),
            "CRM_Value__c" to json.string(Base64.getEncoder().encodeToString(this.value.toByteArray())) // TODO:: Verify
    )
}
