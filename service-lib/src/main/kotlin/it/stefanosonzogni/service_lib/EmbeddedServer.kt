package it.stefanosonzogni.service_lib

import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import it.stefanosonzogni.service_lib.health.Diagnosable
import it.stefanosonzogni.service_lib.health.HealthBodyResponse
import org.slf4j.LoggerFactory

object EmbeddedServer {
    fun createEmbeddedServer(httpPort: Int, serviceName: String, servicesToDiagnose: Map<String, Diagnosable>): NettyApplicationEngine {
        return embeddedServer(Netty, httpPort) {
            val version = StatusService().getVersion()
            val okBodyResponse = ObjectMapper()
                .writeValueAsString(
                    HealthBodyResponse("$serviceName-api", version, HealthBodyResponse.Status.OK.value)
                )
            val koBodyResponse = ObjectMapper()
                .writeValueAsString(
                    HealthBodyResponse("$serviceName-api", version, HealthBodyResponse.Status.KO.value)
                )
            val log = LoggerFactory.getLogger("Health")

            routing {
                get("/health/liveness") {
                    for ((name, svc) in servicesToDiagnose) {
                        if (!svc.isHealthy()) {
                            log.error("liveness: service $name is unhealthy")
                            call.respond(HttpStatusCode.ServiceUnavailable, koBodyResponse)
                            return@get
                        }
                    }
                    call.respond(HttpStatusCode.OK, okBodyResponse)
                }
                get("/health/readiness") {
                    for ((name, svc) in servicesToDiagnose) {
                        if (!svc.isReady()) {
                            log.error("readiness: service $name is not ready")
                            call.respond(HttpStatusCode.ServiceUnavailable, koBodyResponse)
                            return@get
                        }
                    }
                    call.respond(HttpStatusCode.OK, okBodyResponse)
                }
            }
        }
    }
}