from pyspark.sql.types import *
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
engine = RecommendationEngine(sc, "/workspaces/Movies_recommendation_perso/Spark-movie-recommendation-main/app/ml-latest/movies.csv", "/workspaces/Movies_recommendation_perso/Spark-movie-recommendation-main/app/ml-latest/ratings.csv")

# Exemple d'utilisation des méthodes de la classe RecommendationEngine
user_id = engine.create_user(None)

if engine.is_user_known(user_id):
    movie = engine.get_movie(None)
    ratings = engine.get_ratings_for_user(user_id)
    engine.add_ratings(user_id, ratings)
    prediction = engine.predict_rating(user_id, movie.movieId)
    recommendations = engine.recommend_for_user(user_id, 10)