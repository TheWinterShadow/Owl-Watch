# Created by The White Wolf
# Date: 8/21/20
# Time: 7:41 PM


from flask import Flask, render_template

app = Flask(__name__)


@app.route("/")
def load_index():
    return render_template("index.html")


if __name__ == "__main__":
    app.run()

