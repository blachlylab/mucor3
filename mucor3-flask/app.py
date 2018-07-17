from flask import Flask, flash, request, redirect,session, url_for,render_template
import os
from werkzeug.utils import secure_filename
import subprocess
import uuid
import sys
from celery_server import tasks
UPLOAD_FOLDER = '/data/'
ALLOWED_EXTENSIONS = set(['vcf'])
UPLOADED_FILES=set()
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'files' not in request.files:
            flash('No file part')
            return redirect(request.url)
        uploadfiles = request.files.getlist('files')
        # if user does not select file, browser also
        # submit an empty part without filename
        for f in uploadfiles:
            if f.filename == '':
                flash('No selected file')
                return redirect(request.url)
            if f and allowed_file(f.filename):
                filename = secure_filename(f.filename)
                if "uuid" not in session:
                    session["uuid"]=str(uuid.uuid4())
                    os.mkdir(os.path.join(app.config['UPLOAD_FOLDER'],session["uuid"]))
                f.save(os.path.join(app.config['UPLOAD_FOLDER'],session["uuid"],filename))
                if "files" in session:
                    set_files=set(session["files"])
                    set_files.add(f.filename)
                    session["files"]=list(set_files)
                else:
                    session["files"]=[f.filename]
                # UPLOADED_FILES.add(file.filename)
    if "files" in session:
        return render_template("upload.html",files=sorted(session["files"]))
    else:
        return render_template("upload.html")

@app.route('/remove', methods=['POST'])
def remove_upload():
    filename=request.form.get("upload-remove")
    print(filename)
    os.remove(os.path.join(app.config['UPLOAD_FOLDER'],session["uuid"],filename))
    allfiles=session["files"]
    allfiles.remove(filename)
    session["files"]=allfiles
    return redirect("/")

@app.route('/mucorelate', methods=['POST'])
def mucorelate():
    jobs=[]
    for x in session["files"]:
        jobs.append(tasks.atomizer.delay(os.path.join(app.config['UPLOAD_FOLDER'],session["uuid"],x)))
    return str(jobs[0].get())

@app.route('/Result', methods=['GET', 'POST'])
def get_results():
    if request.method == 'POST':
        pass
    return render_template("result.html")


if __name__ == '__main__':
    app.secret_key="myflaskapp".encode("utf-8")
    app.run(debug=True,host='0.0.0.0')
