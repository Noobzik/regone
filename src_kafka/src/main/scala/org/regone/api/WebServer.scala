package org.regone.api

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore}
import org.regone.api.models.ProchainDepartResponse
import org.regone.streaming.StreamProcessing

import java.time.Instant
import scala.jdk.CollectionConverters.asScalaIteratorConverter
object WebServer extends  PlayJsonSupport {
  def routes(streams: KafkaStreams): Route = {
    concat(
      path("depart" /  Segment) { period: String =>
        get {
          val isNumber : Boolean = period.forall(Character.isDigit)
          isNumber match {
            case true =>
              // On définit la KV à partir du StoreName
              val kvProchainsDeparts : ReadOnlyKeyValueStore[String, ProchainDepart] = streams.store(
                StoreQueryParameters.fromNameAndType(
                  StreamProcessing.nextDepartureOfStationStoreName,
                  QueryableStoreTypes.keyValueStore()
                ))

              // Ici on récupère les prochains trains à partir d'une gare donnée, restitué sous la forme d'une liste
              val valueProchainDeparts = kvProchainsDeparts.all().asScala.filter(keyValue => keyValue.key == period).toList

              // Ici on écrit le résultat (A adapter pour aws)
              complete(
                ProchainDepartResponse("", "", "")
              )
            case false =>
              complete(
                HttpResponse(StatusCodes.NotFound, entity = "Not found")
              )
          }
        }
      },

    )
  }

}
