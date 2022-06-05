package org.regone.api.models

import play.api.libs.json.{Json, OFormat}

case class ProchainDepartResponse(
                                 mission: String,
                                 prochainDepart: String,
                                 destination: String
                                 )

object ProchainDepartResponse {
  implicit  val format: OFormat[ProchainDepartResponse] = Json.format[ProchainDepartResponse]
}
