from flask import Flask, Blueprint, render_template, request, jsonify
import json
import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from engine import RecommendationEngine



#Créez un Blueprint Flask :
main = Blueprint('main', __name__)

# Initialisez Spark
findspark.init()

# Définissez la route principale ("/")
@main.route("/", methods=["GET", "POST", "PUT"])
def home():
    return render_template("index.html")

# Définissez la route pour récupérer les détails d'un film
@main.route("/movies/<int:movie_id>", methods=["GET"])
def get_movie(movie_id):
    movie = engine.get_movie(movie_id)
    return jsonify({'movie_id': movie['movieId'], 'title': movie['title']})

# Définissez la route pour ajouter de nouvelles évaluations pour les films
@main.route("/newratings/<int:user_id>", methods=["POST"])
def new_ratings(user_id):
    if not engine.is_user_known(user_id):
        engine.create_user(user_id)
    ratings_data = request.get_json()
    engine.add_ratings(user_id, ratings_data['ratings'])
    return jsonify({'message': 'Ratings added successfully'})

# Définissez la route pour ajouter des évaluations à partir d'un fichier
@main.route("/<int:user_id>/ratings", methods=["POST"])
def add_ratings(user_id):
    file = request.files['file']
    ratings_data = json.loads(file.read().decode('utf-8'))
    engine.add_ratings(user_id, ratings_data['ratings'])
    return jsonify({'message': 'Model retrained successfully'})

# Définissez la route pour obtenir la note prédite d'un utilisateur pour un film
@main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
def movie_ratings(user_id, movie_id):
    prediction = engine.predict_rating(user_id, movie_id)
    return str(prediction)

# Définissez la route pour obtenir les meilleures évaluations recommandées pour un utilisateur
@main.route("/<int:user_id>/recommendations", methods=["GET"])
def get_recommendations(user_id):
    nb_movies = request.args.get('nb_movies', default=10, type=int)
    recommendations = engine.recommend_for_user(user_id, nb_movies)
    return jsonify(recommendations)

# Définissez la route pour obtenir les évaluations d'un utilisateur
@main.route("/ratings/<int:user_id>", methods=["GET"])
def get_ratings_for_user(user_id):
    ratings = engine.get_ratings_for_user(user_id)
    return jsonify(ratings)

# Créez une fonction create_app(spark_context, movies_set_path, ratings_set_path) pour créer l'application Flask
def create_app(spark_context, movies_set_path, ratings_set_path):
    # Initialisez SparkSession
    spark = SparkSession.builder.appName("MovieRecommendation").getOrCreate()
    
    # Créez une instance de la classe RecommendationEngine
    engine = RecommendationEngine(spark_context, movies_set_path, ratings_set_path)
    
    # Créez une instance de l'application Flask
    app = Flask(__name__)
    
    # Enregistrez le Blueprint "main" dans l'application
    app.register_blueprint(main)
    
    # Configurez les options de l'application Flask
    app.config['SECRET_KEY'] = 'your_secret_key'
    
    return app

