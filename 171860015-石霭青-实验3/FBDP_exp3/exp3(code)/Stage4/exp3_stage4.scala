package oasans.com
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, NaiveBayes, SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.{DecisionTree, GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd._


object exp3_stage4 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("exp3_stage4").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    // Load training data.
    val train_lines = sc.textFile("hdfs://localhost:9000/input_exp3/train_after.csv")

    // 处理label、features
    val data = train_lines.map{ line =>
      val info = line.split(",")
      val label = info(4).toInt
      val features = Vectors.dense(info.slice(1, 3).map(_.toDouble))
      LabeledPoint(label, features)
    }

    // Split data into training (70%) and test (30%).
    val splits = data.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)


    // Run training algorithm to build the model
    // 1. SVM
//    val numIterations = 100
//    val model = SVMWithSGD.train(training, numIterations)

    // 2. Logistic regression
//    val model = new LogisticRegressionWithLBFGS()
//      .setNumClasses(10)
//      .run(training)

    // 3. Naive Bayes
    //val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    // 4. DecisionTree
//    val numClasses = 2
//    val categoricalFeaturesInfo = Map[Int, Int]()
//    val impurity = "gini"
//    val maxDepth = 5
//    val maxBins = 32
//    val model = DecisionTree.trainClassifier(training, numClasses, categoricalFeaturesInfo,
//      impurity, maxDepth, maxBins)

    // 5. RandomForest
//    val numClasses = 2
//    val categoricalFeaturesInfo = Map[Int, Int]()
//    val numTrees = 3 // Use more in practice.
//    val featureSubsetStrategy = "auto" // Let the algorithm choose.
//    val impurity = "gini"
//    val maxDepth = 4
//    val maxBins = 32
//    val model = RandomForest.trainClassifier(training, numClasses, categoricalFeaturesInfo,
//      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // 6. Gradient-Boosted Trees (GBTs)
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 5
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
    val model = GradientBoostedTrees.train(training, boostingStrategy)


    // Clear the default threshold.
    //model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)

    evaluating(metrics, "Gradient-Boosted Trees (GBTs)")

  }

  def evaluating(metrics: BinaryClassificationMetrics, alName: String): Unit={

    println("Evaluation of "+alName)
    // 输出各评估指标
    // Precision-Recall Curve
    val PRC = metrics.pr
    println("Precision-Recall")
    PRC.foreach{case(p, r)=>
      println(s"Precision: $p, recall: $r")
    }
    println("\n")

    // Precision by threshold
    val precision = metrics.precisionByThreshold
    println("Precision by threshold")
    precision.foreach{case(t, p)=>
      println(s"Threshold: $t, Precision: $p")
    }
    println("\n")

    // Recall by threshold
    val recall = metrics.recallByThreshold
    println("Recall by threshold")
    recall.foreach{case(t, r)=>
      println(s"Threshold: $t, Recall: $r")
    }
    println("\n")

    // F-measure
    val f1Score = metrics.fMeasureByThreshold
    println("F-measure by threshold(Beta=1)")
    f1Score.foreach{case(t, f)=>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }
    println("\n")

    val beta = 0.5
    val fScore = metrics.fMeasureByThreshold(beta)
    println("F-measure by threshold(Beta=0.5)")
    f1Score.foreach{case(t, f)=>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }
    println("\n")

    // AUPRC
    val auPRC = metrics.areaUnderPR
    println("Area under precision-recall curve = "+ auPRC)
    println("\n")

    // Compute thresholds used in ROC and PR curves
    val thresholds = precision.map(_._1)

    // ROC Curve
    val roc = metrics.roc

    // AUROC
    val auROC = metrics.areaUnderROC
    println("Area under ROC = "+ auROC)
  }


}
