package org.regone.streaming.models


import play.api.libs.json.{Json, OFormat}

case class nextDepartures(
                         miss: String,
                         num: String,
                         term: String,
                         depart: String,
                         type_horaire: String
                         )

object nextDepartures {
  implicit val format: OFormat[nextDepartures] = Json.format[nextDepartures]
}