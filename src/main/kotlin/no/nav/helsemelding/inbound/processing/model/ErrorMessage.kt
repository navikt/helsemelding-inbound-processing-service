package no.nav.helsemelding.inbound.processing.model

import kotlinx.serialization.Serializable
import kotlin.time.Instant

@Serializable
data class ErrorMessage(
    val processedAt: Instant,
    val sourceSystem: String,
    val errors: List<ProcessingError>,
    val originalMessage: OriginalMessage
)
