package no.nav.helsemelding.inbound.processing.model

import kotlinx.serialization.Serializable

@Serializable
data class ProcessingError(
    val category: ErrorCategory,
    val code: ErrorCode,
    val message: String
)
