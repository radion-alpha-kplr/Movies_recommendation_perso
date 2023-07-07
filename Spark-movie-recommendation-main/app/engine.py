"""from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SQLContext

class RecommendationEngine:
    def __init__(self, sc, movies_set_path, ratings_set_path):
        self.sc = sc
        self.sqlContext = SQLContext(sc)

        # Chargement des ensembles de données de films et d'évaluations à partir des fichiers CSV
        self.movies_df = self.sqlContext.read.csv(movies_set_path, header=True, inferSchema=True)
        self.ratings_df = self.sqlContext.read.csv(ratings_set_path, header=True, inferSchema=True)

        # Définition du schéma des données
        self.movies_schema = StructType([
            StructField("movieId", IntegerType(), True),
            StructField("title", StringType(), True)
        ])
        self.ratings_schema = StructType([
            StructField("userId", IntegerType(), True),
            StructField("movieId", IntegerType(), True),
            StructField("rating", DoubleType(), True)
        ])

        # Conversion des colonnes dans le bon format
        self.movies_df = self.movies_df.select(col("movieId").cast(IntegerType()), col("title").cast(StringType()))
        self.ratings_df = self.ratings_df.select(col("userId").cast(IntegerType()), col("movieId").cast(IntegerType()), col("rating").cast(DoubleType()))

        # Entraînement initial du modèle ALS
        self.__train_model()

    def create_user(self, user_id=None):
        if user_id is None:
            # Génération automatique d'un nouvel identifiant d'utilisateur
            max_user_identifier = self.ratings_df.agg({"userId": "max"}).collect()[0][0]
            if max_user_identifier is None:
                max_user_identifier = 0
            user_id = max_user_identifier + 1
        return user_id

    def is_user_known(self, user_id):
        return user_id is not None and user_id <= self.ratings_df.agg({"userId": "max"}).collect()[0][0]

    def get_movie(self, movie_id=None):
        if movie_id is None:
            # Sélection d'un échantillon aléatoire d'un film
            return self.movies_df.sample(False, 0.01)
        else:
            return self.movies_df.filter(self.movies_df.movieId == movie_id)

    def get_ratings_for_user(self, user_id):
        return self.ratings_df.filter(self.ratings_df.userId == user_id)

    def add_ratings(self, user_id, ratings):
        # Création d'un nouveau dataframe à partir de la liste de ratings
        new_ratings_df = self.sqlContext.createDataFrame(ratings, self.ratings_schema)

        # Ajout des nouvelles évaluations au dataframe existant
        self.ratings_df = self.ratings_df.union(new_ratings_df)

        # Division des données en ensembles d'entraînement et de test
        training, test = self.ratings_df.randomSplit([0.8, 0.2])

        # Re-entraînement du modèle
        self.__train_model()

    def predict_rating(self, user_id, movie_id):
        rating_df = self.sqlContext.createDataFrame([(user_id, movie_id)], self.ratings_schema)

        # Utilisation du modèle pour prédire l'évaluation
        prediction_df = self.model.transform(rating_df)

        if prediction_df.count():
            return prediction_df.select("prediction").collect()[0][0]
        else:
            return -1        

    def __train_model(self):
    # Création de l'instance de l'algorithme ALS avec les paramètres maxIter et regParam
        als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")

        # Entraînement du modèle en utilisant le dataframe d'entraînement
        self.model = als.fit(self.training)

        # Évaluation du modèle en calculant l'erreur quadratique moyenne (RMSE)
        predictions = self.model.transform(self.test)
        evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
        self.rmse = evaluator.evaluate(predictions)

    def __evaluate(self):
        # Affichage de la valeur de l'erreur quadratique moyenne (RMSE)
        print("RMSE:", self.rmse)

    def __init__(self, sc, movies_set_path, ratings_set_path):
        self.sc = sc
        self.sqlContext = SQLContext(sc)

        # Chargement des ensembles de données de films et d'évaluations à partir des fichiers CSV
        self.movies_df = self.sqlContext.read.csv(movies_set_path, header=True, inferSchema=True)
        self.ratings_df = self.sqlContext.read.csv(ratings_set_path, header=True, inferSchema=True)

        # Définition du schéma des données
        self.movies_schema = StructType([
            StructField("movieId", IntegerType(), True),
            StructField("title", StringType(), True)
        ])
        self.ratings_schema = StructType([
            StructField("userId", IntegerType(), True),
            StructField("movieId", IntegerType(), True),
            StructField("rating", DoubleType(), True)
        ])

        # Conversion des colonnes dans le bon format
        self.movies_df = self.movies_df.select(col("movieId").cast(IntegerType()), col("title").cast(StringType()))
        self.ratings_df = self.ratings_df.select(col("userId").cast(IntegerType()), col("movieId").cast(IntegerType()), col("rating").cast(DoubleType()))

        # Division des données en ensembles d'entraînement et de test
        self.training, self.test = self.ratings_df.randomSplit([0.8, 0.2])

        # Entraînement initial du modèle ALS
        self.__train_model()

# Création de la session Spark
spark = SparkSession.builder.appName("MovieRecommendation").getOrCreate()

# Assignation du contexte Spark à la variable sc
sc = spark.sparkContext

# Création d'une instance de la classe RecommendationEngine
engine = RecommendationEngine(sc, "Spark-movie-recommendation-main/app/ml-latest/movies.csv", "Spark-movie-recommendation-main/app/ml-latest/ratings.csv")

# Exemple d'utilisation des méthodes de la classe RecommendationEngine
user_id = engine.create_user(None)

if engine.is_user_known(user_id):
    movie = engine.get_movie(None)
    ratings = engine.get_ratings_for_user(user_id)
    engine.add_ratings(user_id, ratings)
    prediction = engine.predict_rating(user_id, movie.movieId)
    recommendations = engine.recommend_for_user(user_id, 10)"""


from pyspark.sql.types import *
from pyspark.sql.functions import explode, col
 
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
 
from pyspark.sql import SQLContext
 
import random
 
class RecommendationEngine:
 
    """
        Create new User.
    """
    def create_user(self, user_id):
        if user_id == None:
            self.max_user_identifier = self.max_user_identifier + 1
        elif user_id > self.max_user_identifier:
            self.max_user_identifier = user_id
        return self.max_user_identifier
 
    """
        Check is a user known.
    """
    def is_user_known(self, user_id):
        return user_id != None and user_id <= self.max_user_identifier
 
    """
        Get a movie.
    """
    def get_movie(self, movie_id):
        if movie_id == None:
            best_movies_struct = [StructField("movieId", IntegerType(), True),
                        StructField("title", StringType(), True),
                        StructField("count", IntegerType(), True)]
            best_movies_df = self.spark.createDataFrame(self.most_rated_movies, StructType(best_movies_struct))
            return best_movies_df.sample(False, fraction=0.05).select("movieId", "title").limit(1)
        else:
            return self.movies_df.filter("movieId == " + str(movie_id))
 
    """
        Get ratings for user.
    """
    def get_ratings_for_user(self, user_id):
        return self.ratings_df.filter("userId == " + str(user_id))
 
    """
        Adds new ratings to the model dataset and train the model again.
    """
    def add_ratings(self, user_id, ratings):
        rating_struct = [StructField("movieId", IntegerType(), True),
            StructField("userId", IntegerType(), True),
            StructField("rating", DoubleType(), True)]
        
        ratings_list = list(ratings)
        print("Add {} new ratings to train the model".format(len(ratings_list)))
 
        new_ratings_df = self.spark.createDataFrame(ratings_list, StructType(rating_struct))
        self.ratings_df = self.ratings_df.union(new_ratings_df)
 
        # Splitting training data
        self.training, self.test = self.ratings_df.randomSplit([0.8, 0.2], seed=12345)
        self.__train_model()
 
    """
        Given a user_id and a movie_id, predict ratings for it.
    """
    def predict_rating(self, user_id, movie_id):
        data = [(user_id, movie_id)]
 
        rating_struct = [StructField("userId", IntegerType(), True),
            StructField("movieId", IntegerType(), True)]
        rating_df = self.spark.createDataFrame(data, StructType(rating_struct))
 
        prediciton_df = self.model.transform(rating_df)
        if (prediciton_df.count() == 0):
            return -1
        return prediciton_df.collect()[0].asDict()["prediction"]
 
    """
        Returns the top recommendations for a given user.
    """
    def recommend_for_user(self, user_id, nb_movies):
        user_df = self.spark.createDataFrame([user_id], IntegerType()).withColumnRenamed("value", "userId")
        ratings = self.model.recommendForUserSubset(user_df, nb_movies)
        user_recommandations = ratings.select(
             explode(col("recommendations").movieId).alias("movieId")
        )
        return user_recommandations.join(self.movies_df, "movieId").drop("genres").drop("movieId")
 
    """
        Train the model with ALS.
    """
    def __train_model(self):
        als = ALS(maxIter=self.max_iter,
                  regParam=self.reg_param, \
                  implicitPrefs=False, \
                  userCol="userId", \
                  itemCol="movieId", \
                  ratingCol="rating", \
                  coldStartStrategy="drop")
 
        self.model = als.fit(self.training)
        self.__evaluate()
 
    """
        Evaluate the model by calculating the Root-mean-square error.
    """
    def __evaluate(self):
        predictions = self.model.transform(self.test)
        evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
        self.rmse = evaluator.evaluate(predictions)
        print("Root-mean-square error = " + str(self.rmse))
 
    """
        Load datasets and train the model.
    """
    def __init__(self, sc, movies_set_path, ratings_set_path):
        self.spark = SQLContext(sc).sparkSession
        
        # Get hyper parameters from command line
        self.max_iter = 9
        self.reg_param = 0.05
 
        print("MaxIter {}, RegParam {}.".format(self.max_iter, self.reg_param))
 
        # Define schema for movies dataset
        movies_struct = [StructField("movieId", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("genres", StringType(), True)]
 
        movies_schema = StructType(movies_struct)
 
        # Define schema for ratings dataset
        ratings_struct = [StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", DoubleType(), True),
        StructField("timestamp", IntegerType(), True)]
 
        ratings_schema = StructType(ratings_struct)
 
        # Read movies from Local File System
        self.movies_df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .schema(movies_schema) \
            .load('file:///'+movies_set_path)
 
        self.movies_count = self.movies_df.count()
        print("Number of movies : {}.".format(self.movies_count))
 
        # Read ratings from Local File System
        self.ratings_df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .schema(ratings_schema) \
            .load('file:///'+ratings_set_path) \
            .drop("timestamp")
 
        self.max_user_identifier = self.ratings_df.select('userId').distinct().sort(col("userId").desc()).limit(1).take(1)[0].userId
        print("Max user id : {}.".format(self.max_user_identifier))
 
        self.most_rated_movies = self.movies_df \
            .join(self.ratings_df, "movieId") \
            .groupBy(col("movieId"), col("title")).count().orderBy("count", ascending=False) \
            .limit(200).collect()
 
        # Splitting training data
        self.training, self.test = self.ratings_df.randomSplit([0.8, 0.2], seed=12345)
 
        self.__train_model()