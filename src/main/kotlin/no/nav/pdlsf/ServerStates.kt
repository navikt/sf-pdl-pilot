package no.nav.pdlsf

sealed class ServerStates {
    object KeystoreIssues : ServerStates()
    object SalesforceIssues : ServerStates()
    object KafkaIssues : ServerStates()
    object KafkaConsumerIssues : ServerStates()
    object IntegrityIssues : ServerStates()
    object PreStopHookActive : ServerStates()
}

object ServerState {
    private var states: MutableSet<ServerStates> = mutableSetOf()

    fun flag(s: ServerStates) { states.add(s) }

    fun isOk(): Boolean = states
            .minus(ServerStates.SalesforceIssues)
            .minus(ServerStates.KafkaIssues)
            .minus(ServerStates.KafkaConsumerIssues)
            .isEmpty()

    fun preStopIsActive(): Boolean = states.contains(ServerStates.PreStopHookActive)

    fun reset() { states = mutableSetOf() }
}
