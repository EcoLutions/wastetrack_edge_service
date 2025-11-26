"""Flask application entry point for the Smart Band Edge Service."""

from flask import Flask

app = Flask(__name__)

first_request = True

if __name__ == "__main__":
    app.run(debug=True)
