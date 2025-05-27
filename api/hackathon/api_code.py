import os, io, zipfile
from flask import Flask, render_template, request
from flask import redirect, url_for, session, send_from_directory, flash
from flask import make_response
from werkzeug.utils import secure_filename
from hashlib import sha256
from db.website import db, NewsletterSubscriber
from db.website import User, Submission
from local_secrets import UPLOAD_FOLDER, ALLOWED_EXTENSIONS

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