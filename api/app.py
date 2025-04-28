from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, request, render_template_string
from flask_cors import CORS
from flasgger import Swagger, swag_from
import pandas as pd
import sys
import os, json

from local_secrets import CODE_PATH
from custom_decorators import log_request

sys.path.append(CODE_PATH)
import functions


app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

template = {
  "swagger": "2.0",
  "info": {
    "title": "ProVe API",
    "description": "API for ProVe, developed by the King's Knowledge Graph Lab.",
    "version": "1.1.1"
  },
  "host": "kclwqt.sites.er.kcl.ac.uk",
  "schemes": ["https"],
  "basePath": "/",
  "consumes": ["application/json"],
  "produces": ["application/json"],
  "paths": {},
  "headers": [("Access-Control-Allow-Origin", "*")]
}
Swagger(app, template=template)
PATH = Path(__file__).parent.absolute()


@app.route('/api/config', methods=['GET'])
@swag_from(f'{PATH}/docs/api/config.yml')
@log_request
def get_config():
    try:
        config_json = functions.get_config_as_json()
        return config_json, 200, {'Content-Type': 'application/json'}
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/items/getSimpleResult', methods=['GET'])
@swag_from(f'{PATH}/docs/api/items/getSimpleResult.yml')
@log_request
def getSimpleResult():
    target_id = request.args.get('qid')
    if target_id is None:
        return jsonify({"error": "Missing 'id' parameter"}), 400
    try:
        item = functions.GetItem(target_id)
        return jsonify(item), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/items/checkItemStatus', methods=['GET'])
@swag_from(f'{PATH}/docs/api/items/checkItemStatus.yml')
@log_request
def CheckItemStatus():
    target_id = request.args.get('qid')
    if target_id is None:
        return jsonify({"error": "Missing 'id' parameter"}), 400
    try:
        item = functions.CheckItemStatus(target_id)
        return jsonify(item), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/items/getCompResult', methods=['GET'])
@swag_from(f'{PATH}/docs/api/items/comprehensiveResults.yml')
@log_request
def comprehensive_results():
    target_id = request.args.get('qid')
    try:
        item = functions.comprehensive_results(target_id)
        return item, 200
    except Exception as e:
        return jsonify({"error": f"{type(e).__name__}: {str(e)}"}), 500


@app.route('/api/items/summary', methods=['GET'])
@swag_from(f'{PATH}/docs/api/items/summary.yml')
@log_request
def get_summary():
    target_id = request.args.get('qid', None)

    try:
        if target_id:
            summary = functions.get_summary(target_id)
            return jsonify(summary), 200
        else:
            return jsonify({'error': 'Invalid request'}), 400
    except Exception as e:
        return jsonify({'error': f'{type(e).__name__}: {str(e)}'}), 500


@app.route('/api/items/history', methods=['GET'])
@log_request
def get_history():
    target_id = request.args.get('qid', None)

    # Get history by date
    from_date = request.args.get('from', None)
    to_date = request.args.get('to', datetime.now())

    # Get history by index
    index = request.args.get('index', None)

    try:
        if target_id:
            if isinstance(to_date, str):
                to_date = datetime.strptime(to_date, '%Y-%m-%d')
                if to_date > datetime.utcnow():
                    return jsonify({'error': 'Invalid request (to after today)'}), 400

            if from_date:
                from_date = datetime.strptime(from_date, '%Y-%m-%d')
                if from_date > to_date:
                    return jsonify({'error': 'Invalid request (from after to)'}), 400

            if index:
                try:
                    index = int(index)
                except ValueError:
                    return jsonify({'error': 'Invalid request (index has to be integer)'}), 400

            history = functions.get_history(target_id, from_date, to_date, index)
            return jsonify(history), 200
        else:
            return jsonify({'error': 'Invalid request'}), 400
    except Exception as e:
        return jsonify({'error': f'{type(e).__name__} {str(e)}'}), 500


@app.route('/api/task/checkQueue', methods=['GET'])
@swag_from(f'{PATH}/docs/api/task/checkQueue.yml')
@log_request
def checkQueue():
    item = functions.checkQueue()
    return jsonify(item), 200


@app.route('/api/task/checkCompleted', methods=['GET'])
@swag_from(f'{PATH}/docs/api/task/checkCompleted.yml')
@log_request
def checkCompleted():
    items = functions.checkCompleted()
    # If items is a list and contains more than 1000 elements, only return the last 1000 elements.
    if isinstance(items, list) and len(items) > 1000:
        items = items[-1000:]
    return jsonify(items), 200


@app.route('/api/task/checkErrors', methods=['GET'])
@swag_from(f'{PATH}/docs/api/task/checkErrors.yml')
@log_request
def checkErrors():
    item = functions.checkErrors()
    # If items is a list and contains more than 1000 elements, only return the last 1000 elements.
    if isinstance(item, list) and len(item) > 1000:
        item = item[-1000:]
    return jsonify(item), 200


@app.route('/api/requests/requestItem', methods=['GET'])
@swag_from(f'{PATH}/docs/api/requests/requestItem.yml')
@log_request
def requestItem():
    target_id = request.args.get('qid')
    username = request.args.get('username')
    if not target_id:
        return jsonify({"error": "Missing 'qid' parameter"}), 400
    if not target_id.startswith('Q'):
        return jsonify({"error": "Invalid 'qid' format"}), 400
    try:
        result = functions.requestItemProcessing(target_id)
        return jsonify({
            "reuqest_result": "success",
            "message": result,
            "qid": target_id,
            "username": username
        }), 202

    except Exception as e:
        app.logger.error(f"Error processing request for QID {target_id}: {str(e)}")
        return jsonify({
            "reuqest_result": "error",
            "message": "An error occurred while processing your request",
            "error": str(e)
        })


@app.route('/api/worklist/generationBasics', methods=['GET'])
@swag_from(f'{PATH}/docs/api/worklist/generationBasics.yml')
@log_request
def GenerationBasics():
    try:
        app.logger.info("GenerationBasics API called")
        item = functions.generation_worklists()
        app.logger.info("GenerationBasics completed successfully")
        return jsonify(json.loads(item)), 200
    except Exception as e:
        app.logger.error(f"Error in GenerationBasics: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/page/worklist/generationBasics', methods=['GET'])
@swag_from(f'{PATH}/docs/page/worklist/generationBasics.yml')
@log_request
def pageViewforWorklist():
    item = functions.generation_worklists()
    data = item
    data = json.loads(item)
    html_template = """
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>worklist</title>
        <style>
            table {
                border-collapse: collapse;
                width: 100%;
                margin-bottom: 20px;
            }
            th, td {
                border: 1px solid black;
                padding: 8px;
                text-align: left;
            }
            th {
                background-color: #f2f2f2;
            }
            h2 {
                margin-top: 30px;
            }
        </style>
    </head>
    <body>
        <h1>worklist</h1>
        {% for key, df_json in data.items() %}
            <h2>{{ key }}</h2>
            <table>
                <thead>
                    <tr>
                        {% for column in df_json[0].keys() %}
                            <th>{{ column }}</th>
                        {% endfor %}
                    </tr>
                </thead>
                <tbody>
                    {% for row in df_json %}
                        <tr>
                            {% for value in row.values() %}
                                <td>{{ value }}</td>
                            {% endfor %}
                        </tr>
                    {% endfor %}
                </tbody>
            </table>
        {% endfor %}
    </body>
    </html>
    """
    return render_template_string(html_template, data=data)


@app.route('/page/worklist/pagePileList', methods=['GET'])
@swag_from(f'{PATH}/docs/page/worklist/pagePileList.yml')
@log_request
def page_view_for_page_pile():
    # Call the function to generate worklist data
    item = functions.generation_worklist_pagePile()

    # Load the JSON data into a Python object
    data = json.loads(item)

    # HTML template with embedded Jinja2 syntax
    html_template = """
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Worklist</title>
        <style>
            table {
                border-collapse: collapse;
                width: 100%;
                margin-bottom: 20px;
            }
            th, td {
                border: 1px solid black;
                padding: 8px;
                text-align: left;
            }
            th {
                background-color: #f2f2f2;
            }
            h2 {
                margin-top: 30px;
            }
        </style>
    </head>
    <body>
        <h1>Worklist</h1>
        <table>
            <thead>
                <tr>
                    {% if data %}
                        {% for column in data[0].keys() %}
                            <th>{{ column }}</th>
                        {% endfor %}
                    {% endif %}
                </tr>
            </thead>
            <tbody>
                {% for row in data %}
                    <tr>
                        {% for value in row.values() %}
                            <td>{{ value }}</td>
                        {% endfor %}
                    </tr>
                {% endfor %}
            </tbody>
        </table>
    </body>
    </html>
    """

    # Render the HTML template with the data
    return render_template_string(html_template, data=data)


@app.route('/page/plot', methods=['GET'])
@swag_from(f'{PATH}/docs/page/plot.yml')
@log_request
def plotingPilot():
    item = functions.plot_status()
    return render_template_string(item)


#################### HACKATHON ####################
import os, io, zipfile
from flask import Flask, render_template, request
from flask import redirect, url_for, session, send_from_directory, flash
from flask import make_response
from werkzeug.utils import secure_filename
from hashlib import sha256
from db.website import db, NewsletterSubscriber
from db.website import User, Submission
from local_secrets import UPLOAD_FOLDER, ALLOWED_EXTENSIONS

app = Flask(__name__)
app.secret_key = sha256(os.urandom(16)).hexdigest()
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data.db'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

db.init_app(app)

if not os.path.exists('data.db'):
    with app.app_context():
        db.create_all()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def is_safe_zip(file_storage):
    try:
        file_bytes = file_storage.read()
        zip_file = zipfile.ZipFile(io.BytesIO(file_bytes))
        for zip_info in zip_file.infolist():
            if zip_info.is_dir():
                continue
        file_storage.stream.seek(0)
        return True, None
    except zipfile.BadZipFile:
        return False, "Invalid ZIP file."

@app.route('/logout', methods=['POST'])
def logout():
    session.pop('username', None)
    return redirect(url_for('hackathon'))

@app.route('/upload_file', methods=['POST'])
def upload_file():
    try:
        if 'username' not in session:
            return redirect(url_for('hackathon'))

        file = request.files.get('submission')
        if not file or not allowed_file(file.filename):
            flash("Invalid file format. Only .zip allowed.", "error")
            return redirect(url_for('hackathon'))

        valid, error = is_safe_zip(file)
        if not valid:
            flash(error, "error")
            return redirect(url_for('hackathon'))

        user = User.query.filter_by(username=session['username']).first()
        filename = secure_filename(f"{user.username}_" + file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if os.path.exists(filepath):
            flash("File already exists. Please rename your file.", "error")
            return redirect(url_for('hackathon'))

        file.save(filepath)

        db.session.add(Submission(filename=filename, user=user, upload_time=datetime.now()))
        db.session.commit()

        flash("File uploaded successfully!", "success")
        return redirect(url_for('hackathon'))
    except Exception as e:
        flash(f"An error occurred: {str(e)}", "error")
        db.session.rollback()
        if os.path.exists(filepath):
            os.remove(filepath)
        return redirect(url_for('hackathon'))

@app.route('/hackathon')
def hackathon():
    user = None
    user_submission = None

    if 'username' in session:
        user = User.query.filter_by(username=session['username']).first()

        submissions = Submission.query.all()
        submissions = [s for s in submissions if user.id == s.user_id]
        submissions = sorted(submissions, key=lambda x: x.upload_time, reverse=True)
        for s in submissions:
            s.filename = s.filename.split("_", 1)[1:]
            s.filename = "_".join(s.filename)
        user_submission = submissions[0] if submissions else None

        return render_template("hackathon.html", user=user, submissions=submissions, user_submission=user_submission)

    return render_template("hackathon.html", user=None, submissions=None, user_submission=None)

@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)


@app.route('/delete_submission/<filename>', methods=['POST'])
def delete_submission(filename):
    filename = session.get('username') + "_" + filename

    if not filename:
        flash("No file to delete.", "error")
        return redirect(url_for('hackathon'))

    if 'username' not in session:
        return redirect(url_for('hackathon'))

    user_id = User.query.filter_by(username=session['username']).first().id
    submission = Submission.query.filter_by(user_id=user_id, filename=filename).first()

    if submission:
        try:
            db.session.delete(submission)
            os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            db.session.commit()
            flash("File deleted successfully!", "success")
        except FileNotFoundError:
            db.session.commit()
        except Exception as e:
            flash(f"An error occurred: {str(e)}", "error")
            db.session.rollback()
            return redirect(url_for('hackathon'))
    else:
        flash("No file to delete.", "error")

    return redirect(url_for('hackathon'))

@app.route('/auth', methods=['POST'])
def auth():
    username = request.form['username']
    password = request.form['password']
    password = bytes(password, 'utf-8')
    password = sha256(password).hexdigest()
    action = request.form['action']

    if action == 'signup':
        if User.query.filter_by(username=username).first():
            flash("User already exists.", "error")
        else:
            try:
                db.session.add(User(username=username, password=password))
                db.session.commit()
                session['username'] = username
                flash("Signed up successfully!", "success")
            except Exception as e:
                flash(f"An error occurred: {str(e)}", "error")
                db.session.rollback()
                return redirect(url_for('hackathon'))

    elif action == 'login':
        user = User.query.filter_by(username=username, password=password).first()
        if user:
            session['username'] = username
            flash("Logged in successfully!", "success")
        else:
            flash("Invalid login credentials.", "error")

    return redirect(url_for('hackathon'))


@app.route('/')
@app.route('/ProVe')
def prove():
    return render_template('prove.html')

@app.route('/subscribe_newsletter', methods=['POST'])
def subscribe_newsletter():
    name = request.form['name']
    email = request.form['email']

    if not NewsletterSubscriber.query.filter_by(email=email).first():
        subscriber = NewsletterSubscriber(name=name, email=email)
        db.session.add(subscriber)
        db.session.commit()

    return make_response('', 204)




if __name__ == '__main__':
    app.run(debug=True)

