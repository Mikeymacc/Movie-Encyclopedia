from pymongo import MongoClient
import pandas as pd
import boto3
import os
import tkinter as tk
from tkinter import simpledialog, messagebox, scrolledtext, StringVar, Radiobutton
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv

load_dotenv()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION")

class MovieEncyclopedia:
    def __init__(self, db_choice, db_uri='mongodb://localhost:27017/', region_name='us-west-2'):
        self.db_choice = db_choice
        if db_choice == 'mongodb':
            self.client = MongoClient(db_uri)
            self.db = self.client['movie_encyclopedia_db']
            self.movies = self.db.movies
        elif db_choice == 'dynamodb':
            self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
            self.table_name = 'Movies'
            self.ensure_table_exists()
            self.table = self.dynamodb.Table(self.table_name)

    def ensure_table_exists(self):
        try:
            self.table = self.dynamodb.Table(self.table_name)
            self.table.load()
        except self.dynamodb.meta.client.exceptions.ResourceNotFoundException:
            print(f"Table '{self.table_name}' does not exist. Creating table...")
            self.table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {'AttributeName': 'name', 'KeyType': 'HASH'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'name', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            self.table.wait_until_exists()
            print(f"Table '{self.table_name}' created successfully.")


    def load_movies_from_csv(self, csv_file_path):
        df = pd.read_csv(csv_file_path)
        df['casts'] = df['casts'].apply(lambda x: x.split(','))
        df['directors'] = df['directors'].apply(lambda x: x.split(','))
        df['genre'] = df['genre'].apply(lambda x: x.split(','))
        if self.db_choice == 'mongodb':
            self.movies.drop()
            movies_data = df.to_dict('records')
            self.movies.insert_many(movies_data)
            if self.db_choice == 'dynamodb':
                with self.table.batch_writer() as batch:
                    for _, row in df.iterrows():
                        primary_genre = row['genre'][0] if row['genre'] else 'None'
                        item = {
                            'name': row['name'],
                            'year': str(row['year']),
                            'rating': str(row['rating']),
                            'certificate': row['certificate'],
                            'genre': row['genre'],
                            'primary_genre': primary_genre,
                            'casts': row['casts'],
                            'directors': row['directors']
                        }
                        batch.put_item(Item=item)

    def add_movie(self, movie_data):
        if self.db_choice == 'mongodb':
            if 'rating' in movie_data:
                movie_data['rating'] = float(movie_data['rating'])

            self.movies.insert_one(movie_data)

        elif self.db_choice == 'dynamodb':

            if 'rating' in movie_data and isinstance(movie_data['rating'], Decimal):
                movie_data['rating'] = Decimal(str(movie_data['rating']))
            self.table.put_item(Item=movie_data)

    def update_movie(self, movie_name, update_data):

        if self.db_choice == 'mongodb':

            for key, value in update_data.items():
                if isinstance(value, Decimal):
                    update_data[key] = float(value)


        if self.db_choice == 'mongodb':
            self.movies.update_one({'name': movie_name}, {'$set': update_data})
        elif self.db_choice == 'dynamodb':

            for key, value in update_data.items():
                if isinstance(value, float):
                    update_data[key] = Decimal(str(value))
            self.table.update_item(
                Key={'name': movie_name},
                UpdateExpression="set " + ", ".join([f"{k}= :{k}" for k in update_data.keys()]),
                ExpressionAttributeValues={f":{k}": v for k, v in update_data.items()}
            )

    def delete_movie(self, movie_name):

        if self.db_choice == 'mongodb':
            result = self.movies.delete_one({'name': movie_name})
            if result.deleted_count == 0:
                print("No movie found with that name.")
        elif self.db_choice == 'dynamodb':
            try:
                response = self.table.delete_item(
                    Key={'name': movie_name}
                )
            except Exception as e:
                print(f"Failed to delete movie: {e}")

    def find_movies(self, key, value):
        results = []
        if self.db_choice == 'mongodb':
            query = {key: {'$regex': value, '$options': 'i'}}
            results = list(self.movies.find(query, {"_id": 0}).sort("rating", -1).limit(20))
        elif self.db_choice == 'dynamodb':
            try:

                scan_kwargs = {
                    'FilterExpression': f"contains({key}, :val)",
                    'ExpressionAttributeValues': {':val': value}
                }
                done = False
                start_key = None
                while not done:
                    if start_key:
                        scan_kwargs['ExclusiveStartKey'] = start_key
                    response = self.table.scan(**scan_kwargs)
                    results.extend(response.get('Items', []))
                    start_key = response.get('LastEvaluatedKey', None)
                    done = start_key is None
                results = sorted(results, key=lambda x: float(x['rating']), reverse=True)[:20]
            except Exception as e:
                print(f"Error scanning DynamoDB: {str(e)}")

        return results

    def perform_search(self, key, value):
        if self.db_choice == 'mongodb':
            query = {key: {'$regex': value, '$options': 'i'}}
            results = list(self.movies.find(query, {"_id": 0}).sort("rating", -1).limit(20))
        elif self.db_choice == 'dynamodb':

            response = self.table.scan(
                FilterExpression=f"contains({key}, :val)",
                ExpressionAttributeValues={':val': value}
            )

            sorted_items = sorted(response['Items'], key=lambda x: float(x['rating']), reverse=True)[:20]
            results = sorted_items
        else:
            results = []

        return results

    def get_movie_details(self, movie_name):
        if self.db_choice == 'mongodb':
            return self.movies.find_one({"name": {"$regex": movie_name, "$options": "i"}}, {"_id": 0})
        if self.db_choice == 'dynamodb':
            response = self.table.get_item(
                Key={'name': movie_name}
            )
            return response.get('Item', None)
        else:
            return None

class MovieEncyclopediaGUI:
    def __init__(self, master, encyclopedia):
        self.master = master
        self.encyclopedia = encyclopedia
        master.title("Movie Encyclopedia")
        self.setup_widgets()

    def setup_widgets(self):
        self.operation_var = StringVar(self.master)
        self.operation_var.set("Select Operation")
        self.operation_var.trace("w", self.update_fields)

        operations = [
            "Find movies by actor", "Find movies by director",
            "Find movies by genre", "Find movies by certificate",
            "Get movie details", "Add Movie", "Update Movie", "Delete Movie"
        ]

        tk.OptionMenu(self.master, self.operation_var, *operations).grid(row=0, column=0, columnspan=2)


        self.detail_label = tk.Label(self.master, text="Enter Details:")
        self.details_entry = tk.Entry(self.master)
        self.detail_label.grid(row=1, column=0, sticky='e')
        self.details_entry.grid(row=1, column=1)
        self.detail_label.grid_remove()
        self.details_entry.grid_remove()


        self.name_label = tk.Label(self.master, text="Movie Name:")
        self.movie_name_entry = tk.Entry(self.master)
        self.genre_label = tk.Label(self.master, text="Genres (comma-separated):")
        self.genre_entry = tk.Entry(self.master)
        self.director_label = tk.Label(self.master, text="Director(s) (comma-separated):")
        self.director_entry = tk.Entry(self.master)
        self.rating_label = tk.Label(self.master, text="Rating:")
        self.rating_entry = tk.Entry(self.master)
        self.certificate_label = tk.Label(self.master, text="Certificate:")
        self.certificate_entry = tk.Entry(self.master)


        self.name_label.grid(row=2, column=0)
        self.movie_name_entry.grid(row=2, column=1)
        self.genre_label.grid(row=3, column=0)
        self.genre_entry.grid(row=3, column=1)
        self.director_label.grid(row=4, column=0)
        self.director_entry.grid(row=4, column=1)
        self.rating_label.grid(row=5, column=0)
        self.rating_entry.grid(row=5, column=1)
        self.certificate_label.grid(row=6, column=0)
        self.certificate_entry.grid(row=6, column=1)
        self.name_label.grid_remove()
        self.movie_name_entry.grid_remove()
        self.genre_label.grid_remove()
        self.genre_entry.grid_remove()
        self.director_label.grid_remove()
        self.director_entry.grid_remove()
        self.rating_label.grid_remove()
        self.rating_entry.grid_remove()
        self.certificate_label.grid_remove()
        self.certificate_entry.grid_remove()


        self.execute_button = tk.Button(self.master, text="Execute", command=self.execute_operation, font=("Arial", 16))
        self.execute_button.grid(row=7, column=0, columnspan=2)
        self.text = scrolledtext.ScrolledText(self.master, height=20, width=80, font=("Arial", 14))
        self.text.grid(row=8, columnspan=2)

    def execute_operation(self):
        operation = self.operation_var.get()
        detail = self.details_entry.get().strip()


        if operation == "Find movies by actor":
            self.perform_search("casts", detail)
        elif operation == "Find movies by director":
            self.perform_search("directors", detail)
        elif operation == "Find movies by genre":
            self.perform_search("genre", detail)
        elif operation == "Find movies by certificate":
            self.perform_search("certificate", detail)
        elif operation == "Get movie details":
            self.get_movie_details(detail)
        elif operation == "Add Movie":
            self.add_movie()
        elif operation == "Update Movie":
            self.update_movie()
        elif operation == "Delete Movie":
            self.delete_movie()

    def update_fields(self, *args):
        operation = self.operation_var.get()
        self.detail_label.grid_remove()
        self.details_entry.grid_remove()
        self.name_label.grid_remove()
        self.movie_name_entry.grid_remove()
        self.genre_label.grid_remove()
        self.genre_entry.grid_remove()
        self.director_label.grid_remove()
        self.director_entry.grid_remove()
        self.rating_label.grid_remove()
        self.rating_entry.grid_remove()
        self.certificate_label.grid_remove()
        self.certificate_entry.grid_remove()

        if operation in ["Find movies by actor", "Find movies by director", "Find movies by genre",
                         "Find movies by certificate", "Get movie details"]:
            self.detail_label.grid()
            self.details_entry.grid()
        elif operation in ["Add Movie", "Update Movie"]:
            self.name_label.grid()
            self.movie_name_entry.grid()
            self.genre_label.grid()
            self.genre_entry.grid()
            self.director_label.grid()
            self.director_entry.grid()
            self.rating_label.grid()
            self.rating_entry.grid()
            self.certificate_label.grid()
            self.certificate_entry.grid()
        elif operation == "Delete Movie":
            self.name_label.grid()
            self.movie_name_entry.grid()

    def perform_search(self, key, value):

        results = self.encyclopedia.find_movies(key, value)
        self.text.delete('1.0', tk.END)
        if results:
            for movie in results:
                self.text.insert(tk.END, f"{movie['name']} - Rating: {movie.get('rating', 'N/A')}\n")
        else:
            self.text.insert(tk.END, "No movies found.\n")

    def get_movie_details(self, movie_name):
        movie = self.encyclopedia.get_movie_details(movie_name)
        self.text.delete('1.0', tk.END)
        if movie:
            self.text.insert(tk.END,
                                 f"Name: {movie['name']}\nYear: {movie.get('year', 'N/A')}\nRating: {movie.get('rating', 'N/A')}\nGenre: {', '.join(movie.get('genre', []))}\nCertificate: {movie.get('certificate', 'N/A')}\nDirector: {', '.join(movie.get('directors', []))}\n")
        else:
            self.text.insert(tk.END, "Movie not found.\n")

    def add_movie(self):
        name = self.movie_name_entry.get()
        genres = self.genre_entry.get().split(',')
        directors = self.director_entry.get().split(',')
        certificate = self.certificate_entry.get()
        rating_str = self.rating_entry.get()

        if not name or not rating_str:
            messagebox.showerror("Error", "Movie name and rating are required.")
            return

        try:
            rating = Decimal(rating_str)
        except ValueError:
            messagebox.showerror("Error", "Invalid rating. Please enter a numeric value.")
            return

        genres = [genre.strip() for genre in genres if genre.strip()]
        if not genres:
            messagebox.showerror("Error", "At least one genre is required.")
            return

        movie_data = {
            'name': name,
            'genre': genres,
            'directors': directors,
            'certificate': certificate,
            'rating': rating
        }
        self.encyclopedia.add_movie(movie_data)
        self.text.insert(tk.END, f"Added movie: {name}\n")

    def find_movies(self):
        name = self.movie_name_entry.get()
        results = self.encyclopedia.find_movies('name', name)
        self.text.delete('1.0', tk.END)
        for movie in results:
            self.text.insert(tk.END, f"{movie['name']} - {movie.get('rating', 'N/A')}\n")

    def update_movie(self):
        name = self.movie_name_entry.get()
        new_rating = simpledialog.askfloat("Input", "Enter new rating:", parent=self.master)

        if new_rating is None:
            messagebox.showinfo("Cancelled", "Update operation cancelled.")
            return

        new_rating = Decimal(new_rating)
        update_data = {'rating': new_rating}

        self.encyclopedia.update_movie(name, update_data)
        self.text.insert(tk.END, f"Updated movie: {name} with new rating: {new_rating}\n")


    def delete_movie(self):
        name = self.movie_name_entry.get()
        self.encyclopedia.delete_movie(name)
        self.text.insert(tk.END, f"Deleted movie: {name}\n")


if __name__ == "__main__":
    db_choice = input("Choose database system (mongodb/dynamodb): ").lower()
    root = tk.Tk()
    root.geometry("800x600")
    encyclopedia = MovieEncyclopedia(db_choice)
    app = MovieEncyclopediaGUI(root, encyclopedia)
    root.mainloop()
