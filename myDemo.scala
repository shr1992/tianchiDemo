import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class myDemo {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val file: String = "C:/Users/lq/Desktop/tianchi/tianchi_mobile_recommend_train_user.csv"
    val initialData: RDD[Array[String]] = sc.textFile(file).filter(!_.contains("user_id"))
      .map(_.split(",")).cache()
    val start_date: String = "2014-11-18 0"
    val end_date: String = "2014-12-16 24"
    val pre_train_data: RDD[Array[String]] = get_pre_train_data(initialData, start_date, end_date)
    val user_features = construct_user_features(pre_train_data, start_date, end_date)
    user_features.take(5)
    val item_features = construct_item_features(pre_train_data, start_date, end_date)
    item_features.take(5)
    val user_to_item_features = construct_user_to_item_features(pre_train_data, start_date, end_date)
    user_to_item_features.take(5)


  }

  def get_pre_train_data(initialData: RDD[Array[String]], start: String, end: String) = {
    initialData.filter(p => stringTime.between(p(5), start, end))

  }

  def construct_user_features(pre_train_data: RDD[Array[String]], start_date: String, end_date: String): RDD[(String, Array[String])] = {
    val user_data = pre_train_data.map(p => (p(0), p.drop(1))).groupByKey().cache()
    val user_features = cal_count_time_features(user_data, start_date, end_date, 1, 4)
    user_features
  }

  def construct_item_features(pre_train_data: RDD[Array[String]], start_date: String, end_date: String): RDD[(String, Array[String])] = {
    val item_data = pre_train_data.map(p => {
      val tmp = p.toBuffer
      tmp.remove(1)
      (p(1), tmp.toArray)
    }
    ).groupByKey().cache()
    val item_features = cal_count_time_features(item_data, start_date, end_date, 1, 4)
    item_features

  }

  def construct_user_to_item_features(pre_train_data: RDD[Array[String]], start_date: String, end_date: String): RDD[(String, Array[String])] = {
    val user_to_item_data = pre_train_data.map(p => {
      val key = p(0) + "," + p(1)
      val value = p.drop(2)
      (key, value)
    }).groupByKey().cache()
    val user_to_item_features = cal_count_time_features(user_to_item_data, start_date, end_date, 0, 3)
    user_to_item_features

  }

  //计算输入数据的点击、收藏、购物车、购买次数（分时段）、第一次点击、购买，最后一次点击、购买的时间以及
  //购买、点击比
  //输入：type_col_num：用户操作类型所在列的index　　time_col_num　购买时间所在列的index (0-based)
  def cal_count_time_features(data: RDD[(String, Iterable[Array[String]])], start_date: String, end_date: String, type_col_num: Int, time_col_num: Int): RDD[(String, Array[String])] = {
    val features = data.map(line => {
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
        time_dis = stringTime.distance(end_date, item(time_col_num))
        if (item(type_col_num) == "1") {
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
          if (stringTime.distance(item(time_col_num), first_click) <= 0)
            first_click = item(time_col_num)
          if (stringTime.distance(item(time_col_num), last_click) >= 0)
            last_click = item(time_col_num)

        }
        if (item(type_col_num) == "2") {
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
        if (item(type_col_num) == "3") {
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
        if (item(type_col_num) == "4") {
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
          if (stringTime.distance(item(time_col_num), first_buy) <= 0)
            first_buy = item(time_col_num)

          if (stringTime.distance(item(time_col_num), last_buy) >= 0)
            last_buy = item(time_col_num)

        }

      }
      val value = "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%f"
        .format(click, click_6h, click_12h, click_24h, click_3d, click_7d, favorite, favorite_6h, favorite_12h, favorite_24h, favorite_3d, favorite_7d, cart, cart_6h, cart_12h, cart_24h, cart_3d, cart_7d, buy, buy_6h, buy_12h, buy_24h, buy_3d, buy_7d, stringTime.distance(end_date,first_click), stringTime.distance(end_date,last_click), stringTime.distance(end_date,first_buy),stringTime.distance(end_date,last_buy) , buy.toDouble / click).split(",")
      val key = line._1
      (key, value)

    }
    )
    features
  }
}

object stringTime {
  def between(time: String, start: String, end: String): Boolean = {
    distance(time, start) >= 0 && distance(end, time) >= 0
  }

  def distance(a: String, b: String): Int = {
    val a1 = a.split(" ")
    val b1 = b.split(" ")
    if ((a1.length != 2) || (b1.length != 2)) {
      println("%s and %s 格式有问题".format(a, b))
      return 0
    }

    val a2 = ((a1(0).split("-")(1).toInt - 11) * 30 + a1(0).split("-")(2).toInt) * 24 + a1(1).toInt
    val b2 = ((b1(0).split("-")(1).toInt - 11) * 30 + b1(0).split("-")(2).toInt) * 24 + b1(1).toInt
    a2 - b2

  }

}
