from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, request, render_template_string
from flask_cors import CORS
from flasgger import Swagger, swag_from
import sys
import json

from custom_decorators import log_request, api_required, AsyncAuth
from local_secrets import CODE_PATH
from queue_manager import QueueManager

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


queue_managers = {
    "random": QueueManager('random_collection'),
    "user": QueueManager('user_collection')
}


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
@swag_from(f'{PATH}/docs/api/items/history.yml')
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


@app.route("/api/items/processReference", methods=["POST"])
def process_reference():
    data = request.get_json()
    url = data.get("url")
    sentence = data.get("sentence")

    try:
        result = functions.process_reference(url, sentence)
        return jsonify(result), 200
    except Exception:
        return jsonify({"error": "Something went wrong"}), 400


@app.route("/api/internal/getKey", methods=["GET"])
def get_public_key():
    try:
        return jsonify({"public key": AsyncAuth.get_public_key(True)}), 200
    except Exception:
        return jsonify({"error": "Something went wrong"}), 400


@app.route("/api/internal/getNextQueue", methods=["POST"])
@api_required
def get_next_in_queue():
    data = request.get_json()
    request_id = data.get("uuid", None)

    if request_id is None:
        return jsonify({"error": "each request must have a request id"}), 400

    queue = data.get("queue", "random")
    manager = queue_managers.get(queue)

    try:
        id = manager.get_next_in_queue(request_id)
        return jsonify(id), 200
    except Exception as e:
        return jsonify({"error": "Something went wrong", "exception": str(e)}), 400


if __name__ == '__main__':
    app.run(debug=True)
