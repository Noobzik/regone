package org.regone.streaming.models


import play.api.libs.json.{Json, OFormat}

case class nextDepartures(
                         mission: String,
                         station: String,
                         timeArrival: String,
                         destination: String,
                         direction: String
                         )

object nextDepartures {
  implicit val format: OFormat[nextDepartures] = Json.format[nextDepartures]
}