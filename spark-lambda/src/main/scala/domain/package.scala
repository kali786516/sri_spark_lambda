/**
  * Created by kalit_000 on 09/03/2017.
  */
package object domain {
  case class Activity(timestamp_hour: Long,
                      referrer: String,
                      action: String,
                      prevPage: String,
                      page: String,
                      visitor: String,
                      product: String,
                      inputProps: Map[String, String] = Map()
                     )
  case class ActivityByProduct(product: String,
                               timestamp_hour:Long,
                               purchase_count:Long,
                               add_to_cart_count:Long,
                               page_view_count:Long)

}
