package no.nav.pdlsf

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.list
import mu.KotlinLogging

// internal val json = Json(JsonConfiguration.Stable)
private val log = KotlinLogging.logger { }

/**
 * Please refer to
 * https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_composite_sobjects_collections_create.htm
 */

@Serializable
internal data class SFsObjectRest(
    val allOrNone: Boolean = true,
    val records: List<KafkaMessage>
) {
    fun toJson() = json.stringify(serializer(), this)
}

@Serializable
internal data class KafkaMessage(
    val attributes: SFsObjectRestAttributes = SFsObjectRestAttributes(),
    @SerialName("CRM_Topic__c") val topic: String,
    @SerialName("CRM_Key__c") val key: String,
    @SerialName("CRM_Value__c") val value: String
)

@Serializable
internal data class SFsObjectRestAttributes(
    val type: String = "KafkaMessage__c"
)

sealed class SFsObjectStatusBase
object SFsObjectStatusInvalid : SFsObjectStatusBase()

@Serializable
internal data class SFsObjectStatus(
    val id: String = "",
    val success: Boolean,
    val errors: List<SFObjectError> = emptyList()
) : SFsObjectStatusBase() {
    companion object {
        fun fromJson(data: String): List<SFsObjectStatusBase> = runCatching { json.parse(serializer().list, data) }
                .onFailure {
                    ServerState.flag(ServerStates.SalesforceIssues)
                    log.error { "Parsing of sObject REST response failed - ${it.localizedMessage}" }
                }
                .getOrDefault(listOf(SFsObjectStatusInvalid))
    }
}

@Serializable
internal data class SFObjectError(
    val statusCode: String,
    val message: String,
    val fields: List<String> = emptyList()
)

internal fun List<SFsObjectStatusBase>.isInvalid(): Boolean = filterIsInstance<SFsObjectStatusInvalid>().isNotEmpty()
internal fun List<SFsObjectStatus>.getErrMessage(): String = first { !it.success }.errors.first().message
internal fun List<SFsObjectStatus>.toJson(): String = json.stringify(SFsObjectStatus.serializer().list, this)
