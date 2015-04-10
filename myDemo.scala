
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class myDemo {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val initialData: RDD[Array[String]] = sc.textFile("tianchi_mobile_recommend_train_user.csv").filter(!_.contains("user_id"))
      .map(_.split(",")).cache()
    val start_date: String = "2014-11-18 0"
    val end_date: String = "2014-12-16 24"
    val pre_train_data: RDD[Array[String]] = get_pre_train_data(initialData, start_date, end_date)
    val user_features = construct_user_features(pre_train_data, start_date, end_date)


  }

  def get_pre_train_data(initialData: RDD[Array[String]], start: String, end: String) = {
    initialData.filter(p => stringTime.between(p(5), start, end))

  }

  def construct_user_features(pre_train_data: RDD[Array[String]], start_date: String, end_date: String): RDD[Array[String]] = {
    val user_data = pre_train_data.map(p => (p(0), p.drop(0))).groupByKey().cache()
    val user_features = user_data.map(line => {

      var click = 0
      var click_6h = 0
      var click_12h = 0
      var click_24h = 0
      var click_3d = 0
      var click_7d = 0
      var favorite = 0
      var favorite_6h = 0
      var favorite_12h = 0
      var favorite_24h = 0
      var favorite_3d = 0
      var favorite_7d = 0
      var cart = 0
      var cart_6h = 0
      var cart_12h = 0
      var cart_24h = 0
      var cart_3d = 0
      var cart_7d = 0
      var buy = 0
      var buy_6h = 0
      var buy_12h = 0
      var buy_24h = 0
      var buy_3d = 0
      var buy_7d = 0
      var first_click = end_date
      var first_buy = end_date
      var last_click = start_date
      var last_buy = start_date

      var time_dis = 0
      for (item <- line._2) {
        time_dis = stringTime.distance(end_date, item(4))
        if (item(1) == "1") {
          click += 1
          if (time_dis <= 6)
            click_6h += 1
          if (time_dis <= 12)
            click_12h += 1
          if (time_dis <= 24)
            click_24h += 1
          if (time_dis <= 24 * 3)
            click_3d += 1
          if (time_dis <= 24 * 7)
            click_7d += 1

        }
        if (item(1) == "2") {
          favorite += 1
          if (time_dis <= 6)
            favorite_6h += 1
          if (time_dis <= 12)
            favorite_12h += 1
          if (time_dis <= 24)
            favorite_24h += 1
          if (time_dis <= 24 * 3)
            favorite_3d += 1
          if (time_dis <= 24 * 7)
            favorite_7d += 1
        }
        if (item(1) == "3") {
          cart += 1
          if (time_dis <= 6)
            cart_6h += 1
          if (time_dis <= 12)
            cart_12h += 1
          if (time_dis <= 24)
            cart_24h += 1
          if (time_dis <= 24 * 3)
            cart_3d += 1
          if (time_dis <= 24 * 7)
            cart_7d += 1
        }
        if (item(1) == "4") {
          buy += 1
          if (time_dis <= 6)
            buy_6h += 1
          if (time_dis <= 12)
            buy_12h += 1
          if (time_dis <= 24)
            buy_24h += 1
          if (time_dis <= 24 * 3)
            buy_3d += 1
          if (time_dis <= 24 * 7)
            buy_7d += 1
        }
        if (stringTime.distance(item(4), first_click) <= 0)
          first_click = item(4)
        if (stringTime.distance(item(4), first_buy) <= 0)
          first_buy = item(4)
        if (stringTime.distance(item(4), last_click) >= 0)
          last_click = item(4)
        if (stringTime.distance(item(4), last_buy) >= 0)
          last_buy = item(4)

      }
      "%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s,%d".format(line._1, click, click_6h, click_12h, click_24h, click_3d, click_7d, favorite, favorite_6h, favorite_12h, favorite_24h, favorite_3d, favorite_7d, cart, cart_6h, cart_12h, cart_24h, cart_3d, cart_7d, buy, buy_6h, buy_12h, buy_24h, buy_3d, buy_7d, first_click, last_click, first_buy, last_buy, buy / click).split(",")
    }
    )
    user_features


  }

  def construct_item_features(pre_train_data: RDD[Array[String]], start_date: String, end_date: String): RDD[Array[String]] = {
    val item_data = pre_train_data.map(p => {
      val tmp = p.toBuffer
      tmp.remove(1)
      (p(1), tmp.toArray)
    }
    ).groupByKey().cache()
    val item_features = item_data.map(line => {

      var click = 0
      var click_6h = 0
      var click_12h = 0
      var click_24h = 0
      var click_3d = 0
      var click_7d = 0
      var favorite = 0
      var favorite_6h = 0
      var favorite_12h = 0
      var favorite_24h = 0
      var favorite_3d = 0
      var favorite_7d = 0
      var cart = 0
      var cart_6h = 0
      var cart_12h = 0
      var cart_24h = 0
      var cart_3d = 0
      var cart_7d = 0
      var buy = 0
      var buy_6h = 0
      var buy_12h = 0
      var buy_24h = 0
      var buy_3d = 0
      var buy_7d = 0
      var first_click = end_date
      var first_buy = end_date
      var last_click = start_date
      var last_buy = start_date

      var time_dis = 0
      for (item <- line._2) {
        time_dis = stringTime.distance(end_date, item(4))
        if (item(1) == "1") {
          click += 1
          if (time_dis <= 6)
            click_6h += 1
          if (time_dis <= 12)
            click_12h += 1
          if (time_dis <= 24)
            click_24h += 1
          if (time_dis <= 24 * 3)
            click_3d += 1
          if (time_dis <= 24 * 7)
            click_7d += 1

        }
        if (item(1) == "2") {
          favorite += 1
          if (time_dis <= 6)
            favorite_6h += 1
          if (time_dis <= 12)
            favorite_12h += 1
          if (time_dis <= 24)
            favorite_24h += 1
          if (time_dis <= 24 * 3)
            favorite_3d += 1
          if (time_dis <= 24 * 7)
            favorite_7d += 1
        }
        if (item(1) == "3") {
          cart += 1
          if (time_dis <= 6)
            cart_6h += 1
          if (time_dis <= 12)
            cart_12h += 1
          if (time_dis <= 24)
            cart_24h += 1
          if (time_dis <= 24 * 3)
            cart_3d += 1
          if (time_dis <= 24 * 7)
            cart_7d += 1
        }
        if (item(1) == "4") {
          buy += 1
          if (time_dis <= 6)
            buy_6h += 1
          if (time_dis <= 12)
            buy_12h += 1
          if (time_dis <= 24)
            buy_24h += 1
          if (time_dis <= 24 * 3)
            buy_3d += 1
          if (time_dis <= 24 * 7)
            buy_7d += 1
        }
        if (stringTime.distance(item(4), first_click) <= 0)
          first_click = item(4)
        if (stringTime.distance(item(4), first_buy) <= 0)
          first_buy = item(4)
        if (stringTime.distance(item(4), last_click) >= 0)
          last_click = item(4)
        if (stringTime.distance(item(4), last_buy) >= 0)
          last_buy = item(4)

      }
      "%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s,%d".format(line._1, click, click_6h, click_12h, click_24h, click_3d, click_7d, favorite, favorite_6h, favorite_12h, favorite_24h, favorite_3d, favorite_7d, cart, cart_6h, cart_12h, cart_24h, cart_3d, cart_7d, buy, buy_6h, buy_12h, buy_24h, buy_3d, buy_7d, first_click, last_click, first_buy, last_buy, buy / click).split(",")
    }
    )
    item_features

  }
}

object stringTime {
  def between(time: String, start: String, end: String): Boolean = {
    distance(time, start) >= 0 && distance(end, time) >= 0
  }

  def distance(a: String, b: String): Int = {
    val a1 = a.split(" ")
    val b1 = b.split(" ")

    val a2 = ((a1(0).split("-")(1).toInt - 11) * 30 + a1(0).split("-")(2).toInt) * 24 + a1(1).toInt
    val b2 = ((b1(0).split("-")(1).toInt - 11) * 30 + b1(0).split("-")(2).toInt) * 24 + b1(1).toInt
    a2 - b2

  }

}
