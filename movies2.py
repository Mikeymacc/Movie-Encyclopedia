from pymongo import MongoClient
import pandas as pd
import boto3
import os
import tkinter as tk
from tkinter import simpledialog, messagebox, scrolledtext, StringVar, Radiobutton
from decimal import Decimal, InvalidOperation

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
            self.table_name = 'Movies'  # This should be set before calling ensure_table_exists
            self.ensure_table_exists()  # Now this call can successfully reference self.table_name
            self.table = self.dynamodb.Table(self.table_name)

    def ensure_table_exists(self):
        try:
            self.table = self.dynamodb.Table(self.table_name)
            self.table.load()  # This forces a network call to confirm the table exists.
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
            self.movies.drop()  # Clear existing collection to avoid duplicates
            movies_data = df.to_dict('records')
            self.movies.insert_many(movies_data)
            if self.db_choice == 'dynamodb':
                with self.table.batch_writer() as batch:
                    for _, row in df.iterrows():
                        # Example: taking the first genre as the primary genre for indexing
                        primary_genre = row['genre'][0] if row['genre'] else 'None'
                        item = {
                            'name': row['name'],
                            'year': str(row['year']),
                            'rating': str(row['rating']),
                            'certificate': row['certificate'],
                            'genre': row['genre'],
                            'primary_genre': primary_genre,  # This field would be used for GSI
                            'casts': row['casts'],
                            'directors': row['directors']
                        }
                        batch.put_item(Item=item)

    def add_movie(self, movie_data):
        # Check if the database choice is MongoDB and convert Decimal to float
        if self.db_choice == 'mongodb':
            # Convert all Decimal values to float for MongoDB compatibility
            movie_data['rating'] = float(movie_data['rating'])  # Convert Decimal to float for MongoDB

        # Insert the movie data into the appropriate database
        if self.db_choice == 'mongodb':
            self.movies.insert_one(movie_data)
        elif self.db_choice == 'dynamodb':
            self.table.put_item(Item=movie_data)  # DynamoDB expects Decimal

    def update_movie(self, movie_name, update_data):
        # Check if the database choice is MongoDB and convert Decimal to float
        if self.db_choice == 'mongodb':
            # Convert all Decimal values to float for MongoDB compatibility
            for key, value in update_data.items():
                if isinstance(value, Decimal):
                    update_data[key] = float(value)

        # Perform the update operation on the appropriate database
        if self.db_choice == 'mongodb':
            self.movies.update_one({'name': movie_name}, {'$set': update_data})
        elif self.db_choice == 'dynamodb':
            # For DynamoDB, ensure all numerical values are in Decimal format
            for key, value in update_data.items():
                if isinstance(value, float):  # Convert float to Decimal if necessary
                    update_data[key] = Decimal(str(value))
            self.table.update_item(
                Key={'name': movie_name},
                UpdateExpression="set " + ", ".join([f"{k}= :{k}" for k in update_data.keys()]),
                ExpressionAttributeValues={f":{k}": v for k, v in update_data.items()}
            )

    def delete_movie(self, movie_name):
        """Delete a movie from the database."""
        if self.db_choice == 'mongodb':
            result = self.movies.delete_one({'name': movie_name})
            if result.deleted_count == 0:
                print("No movie found with that name.")
            else:
                print(f"Deleted movie: {movie_name}")
        elif self.db_choice == 'dynamodb':
            try:
                response = self.table.delete_item(
                    Key={'name': movie_name}
                )
                print(f"Deleted movie: {movie_name}")
            except Exception as e:
                print(f"Failed to delete movie: {e}")
    def find_movies(self, key, value):
        if self.db_choice == 'mongodb':
            return list(self.movies.find({key: value}, {"_id": 0}).sort("rating", -1).limit(20))
        elif self.db_choice == 'dynamodb':
            # DynamoDB: Scan to retrieve items matching the filter
            response = self.table.scan(
                FilterExpression=f'contains({key}, :val)',
                ExpressionAttributeValues={':val': value}
            )
            # Sort items manually by 'rating' in descending order and limit to 20 results
            sorted_items = sorted(response['Items'], key=lambda x: float(x['rating']), reverse=True)[:20]
            return sorted_items

        else:
            return []

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
        # Dropdown menu for selecting operation type
        self.operation_var = StringVar(self.master)
        self.operation_var.set("Select Operation")

        operations = [
            "Find movies by actor", "Find movies by director",
            "Find movies by genre", "Find movies by certificate",
            "Get movie details", "Add Movie", "Update Movie", "Delete Movie"
        ]

        tk.OptionMenu(self.master, self.operation_var, *operations).grid(row=0, column=0, columnspan=2)

        # Entry for input
        tk.Label(self.master, text="Enter details:").grid(row=1, column=0)
        self.details_entry = tk.Entry(self.master)
        self.details_entry.grid(row=1, column=1)

        # Entries for adding a movie
        tk.Label(self.master, text="Movie Name:").grid(row=2, column=0)
        self.movie_name_entry = tk.Entry(self.master)
        self.movie_name_entry.grid(row=2, column=1)

        tk.Label(self.master, text="Genres (comma-separated):").grid(row=3, column=0)
        self.genre_entry = tk.Entry(self.master)
        self.genre_entry.grid(row=3, column=1)

        tk.Label(self.master, text="Rating:").grid(row=4, column=0)
        self.rating_entry = tk.Entry(self.master)
        self.rating_entry.grid(row=4, column=1)

        # Button to execute operation
        tk.Button(self.master, text="Execute", command=self.execute_operation, font=("Arial", 16)).grid(row=5, column=0,
                                                                                                        columnspan=2)

        # Text Field for Results
        self.text = scrolledtext.ScrolledText(self.master, height=20, width=80, font=("Arial", 14))
        self.text.grid(row=6, columnspan=2)


    def execute_operation(self):
        operation = self.operation_var.get()
        detail = self.details_entry.get()

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
        rating_str = self.rating_entry.get()

        if not name or not rating_str:
            messagebox.showerror("Error", "Movie name and rating are required.")
            return

        try:
            # Use Decimal for initial data handling; conversion to float is handled in the encyclopedia method for MongoDB
            rating = Decimal(rating_str)
        except ValueError:
            messagebox.showerror("Error", "Invalid rating. Please enter a numeric value.")
            return

        genres = [genre.strip() for genre in genres if genre.strip()]
        if not genres:
            messagebox.showerror("Error", "At least one genre is required.")
            return

        movie_data = {'name': name, 'genre': genres, 'rating': rating}
        self.encyclopedia.add_movie(movie_data)
        self.text.insert(tk.END, f"Added movie: {name}\n")
    def find_movies(self):
        name = self.movie_name_entry.get()
        results = self.encyclopedia.find_movies('name', name)
        self.text.delete('1.0', tk.END)
        for movie in results:
            self.text.insert(tk.END, f"{movie['name']} - {movie.get('rating', 'N/A')}\n")

    def update_movie(self):
        name = self.movie_name_entry.get()  # Get the movie name from the entry widget
        new_rating = simpledialog.askfloat("Input", "Enter new rating:", parent=self.master)

        if new_rating is None:  # Check if the dialog was cancelled
            messagebox.showinfo("Cancelled", "Update operation cancelled.")
            return

        # Convert the rating to Decimal for initial data handling
        new_rating = Decimal(new_rating)

        # Create the update data dictionary
        update_data = {'rating': new_rating}

        # Call the update_movie method of the encyclopedia with the gathered inputs
        self.encyclopedia.update_movie(name, update_data)
        self.text.insert(tk.END, f"Updated movie: {name} with new rating: {new_rating}\n")

    def delete_movie(self):
        name = self.movie_name_entry.get()
        self.encyclopedia.delete_movie(name)
        self.text.insert(tk.END, f"Deleted movie: {name}\n")


# Example usage
if __name__ == "__main__":
    db_choice = input("Choose database system (mongodb/dynamodb): ").lower()
    root = tk.Tk()
    root.geometry("800x600")
    encyclopedia = MovieEncyclopedia(db_choice)
    app = MovieEncyclopediaGUI(root, encyclopedia)
    root.mainloop()
