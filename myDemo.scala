import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD

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

  def in_same_day(a: String, b: String): Boolean = {
    a.split(" ")(0) == b.split(" ")(0)

  }

}

class myDemo {

  val user_file: String = "/data/tianchi/tianchi_mobile_recommend_train_user.csv"
  val item_file: String = "/data/tianchi/tianchi_mobile_recommend_train_item.csv"

  val train_start_date: String = "2014-11-18 0"
  val train_end_date: String = "2014-12-16 24"
  val train_label_date = "2014-12-17 0"

  val test_start_date: String = "2014-11-19 0"
  val test_end_date: String = "2014-12-17 24"
  val test_label_date: String = "2014-12-18 0"

  val real_start_date: String = "2014-11-20 0"
  val real_end_date: String = "2014-12-18 24"

  val negative_sample_fraction: Double = 0.005
  val result_number = 4500

  val num_iteration: Int = 30
  val tree_max_depth: Int = 7

  def main(args: Array[String]) {
    val sc = new SparkContext()

    val initial_user_data: RDD[Array[String]] = sc.textFile(user_file).filter(!_.contains("user_id")).map(_.split(","))
    val item_subset = sc.textFile(item_file).filter(!_.contains("item_id")).map(_.split(",")(0)).collect().toSet
    val reduced_user_data = initial_user_data.filter(p => item_subset.contains(p(1)))


    //("userid,itemid",features)
    val train_feature_vector: RDD[(String, Array[String])] = create_feature_vector(initial_user_data, train_start_date, train_end_date)
    val reduced_train_feature_vector = create_feature_vector(reduced_user_data, train_start_date, train_end_date)

    //("1/0",features)
    val training_data: RDD[(String, Array[String])] = add_label_to_feature_vector(initial_user_data, train_feature_vector, train_label_date)
    //sample
    val training_data_p = training_data.filter(_._1 == "1")
    val training_data_n = training_data.filter(_._1 == "0").sample(false, negative_sample_fraction)
    val training_sample: RDD[(String, Array[String])] = training_data_p.union(training_data_n)

    //change to labeledPoint
    val true_training_data: RDD[LabeledPoint] = training_sample.map(line => {
      LabeledPoint(line._1.toDouble, Vectors.dense(line._2.map(_.toDouble)))
    }).cache()
    val model = training_model(true_training_data)


    val train_pred_value = use_model_to_predict(model, reduced_train_feature_vector)
    val train_pred_positive = get_pred_positive(train_pred_value, result_number)
    val train_label_set = get_label_set(initial_user_data, train_label_date)
    val train_evaluation = calculate_precision_recall_f1(train_pred_positive, train_label_set)
    println("model at train_set: precision = %f , recall = %f , f1 = %f ".format(train_evaluation._1, train_evaluation._2, train_evaluation._3))

    //("userid,itemid",features)
    val test_feature_vector: RDD[(String, Array[String])] = create_feature_vector(reduced_user_data, test_start_date, test_end_date)
    val test_pred_value = use_model_to_predict(model, test_feature_vector)
    val test_pred_positive = get_pred_positive(test_pred_value, result_number)
    val test_label_set = get_label_set(initial_user_data, test_label_date)
    val test_evaluation = calculate_precision_recall_f1(test_pred_positive, test_label_set)
    println("model at test_set: precision = %f , recall = %f , f1 = %f ".format(test_evaluation._1, test_evaluation._2, test_evaluation._3))

    val real_feature_vector = create_feature_vector(reduced_user_data, real_start_date, real_end_date)
    val real_pred = get_pred_positive(use_model_to_predict(model, real_feature_vector), result_number).map(_._1)

    println(real_pred.length)


  }

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
          if (time_dis <= 72)
            click_3d += 1
          if (time_dis <= 168)
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
          if (time_dis <= 72)
            favorite_3d += 1
          if (time_dis <= 168)
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
          if (time_dis <= 72)
            cart_3d += 1
          if (time_dis <= 168)
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
          if (time_dis <= 72)
            buy_3d += 1
          if (time_dis <= 168)
            buy_7d += 1
          if (stringTime.distance(item(time_col_num), first_buy) <= 0)
            first_buy = item(time_col_num)

          if (stringTime.distance(item(time_col_num), last_buy) >= 0)
            last_buy = item(time_col_num)

        }

      }
      val buy_divide_click = (buy + 1).toDouble / (click + 1)

      // if user did not buy or click first/last buy/click   ???!!!
      val value = "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%f"
        .format(click, click_6h, click_12h, click_24h, click_3d, click_7d, favorite, favorite_6h, favorite_12h, favorite_24h, favorite_3d, favorite_7d, cart, cart_6h, cart_12h, cart_24h, cart_3d, cart_7d, buy, buy_6h, buy_12h, buy_24h, buy_3d, buy_7d, stringTime.distance(end_date, first_click), stringTime.distance(end_date, last_click), stringTime.distance(end_date, first_buy), stringTime.distance(end_date, last_buy), buy_divide_click).split(",")
      val key = line._1
      (key, value)

    }
    )
    features
  }

  def get_label_set(initialData: RDD[Array[String]], date: String): Set[String] = {
    initialData.filter(p => stringTime.in_same_day(p(5), date)).filter(p => p(2) == "4").map(p => p(0) + "," + p(1)).collect().toSet
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

  def create_feature_vector(initialData: RDD[Array[String]], start_date: String, end_date: String): RDD[(String, Array[String])] = {
    val pre_train_data: RDD[Array[String]] = get_pre_train_data(initialData, start_date, end_date)
    val user_features = construct_user_features(pre_train_data, start_date, end_date)
    val item_features = construct_item_features(pre_train_data, start_date, end_date)
    val user_to_item_features: RDD[(String, Array[String])] = construct_user_to_item_features(pre_train_data, start_date, end_date)
    //join user_features
    val user_and_utoi_features = user_to_item_features.map(p => {
      val tmp = p._1.split(",")
      (tmp(0), tmp(1) +: p._2) //(user_id, Array(item_id,...))
    }).join(user_features) //(user_id, (Array(item_id,...),user_features))

    //join item_features
    val user_and_utoi_and_item_features: RDD[(String, Array[String])] = user_and_utoi_features.map(p => {
      val tmp = p._2._1.toBuffer
      val item_id = tmp.remove(0)
      val value = (p._1 +: tmp) ++ p._2._2
      (item_id, value.toArray) //(item_id,Array(user_id,...)
    }).join(item_features) //(item_id,(Array(user_id,...),item_features))
      .map(p => {
      val tmp = p._2._1.toBuffer
      val user_id = tmp.remove(0)
      val value = (tmp ++ p._2._2).toArray
      (user_id + "," + p._1, value)
    }) //("user_id,item_id" , all features)
    //user_and_utoi_and_item_features.take(5)

    val all_features: RDD[(String, Array[String])] = user_and_utoi_and_item_features
    all_features
  }

  def add_label_to_feature_vector(initialData: RDD[Array[String]], all_features: RDD[(String, Array[String])], label_day: String): RDD[(String, Array[String])] = {
    val label_set: Set[String] = get_label_set(initialData, label_day)

    val training_data: RDD[(String, Array[String])] = all_features.map(line => {
      val label = if (label_set.contains(line._1)) "1" else "0"
      (label, line._2)
    })
    training_data
  }


  def training_model(true_training_data: RDD[LabeledPoint]) = {
    val boostingStrategy: BoostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.setNumIterations(num_iteration)
    boostingStrategy.treeStrategy.setMaxDepth(tree_max_depth)
    GradientBoostedTrees.train(true_training_data, boostingStrategy)
    // new LogisticRegressionWithLBFGS().setNumClasses(2).run(true_training_data)
  }

  //input: feature_vector:("userid,itemid",features)
  //output: ("userid,itemid",double)
  def use_model_to_predict(model: GradientBoostedTreesModel, feature_vector: RDD[(String, Array[String])]) = {
    feature_vector.map(line => {
      (line._1, model.predict(Vectors.dense(line._2.map(_.toDouble))))
    })

  }

  def get_pred_positive(pred_value: RDD[(String, Double)], result_mumber: Int): Array[(String, Double)] = {
    pred_value.top(result_mumber)(Ordering.by(_._2))
    // val pred_positive = pred_value.filter(_._2 == 1)
  }

  def calculate_precision_recall_f1(pred_positive: Array[(String, Double)], label_set: Set[String]) = {
    val true_positive_num = pred_positive.filter(line => label_set.contains(line._1)).length
    val precision = true_positive_num.toDouble / pred_positive.length
    val recall = true_positive_num.toDouble / label_set.size
    val f1 = 2 * precision * recall / (precision + recall)
    (precision, recall, f1)
  }
}

