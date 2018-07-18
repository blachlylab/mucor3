from flask import Flask, flash, request, redirect,session, url_for,render_template
import os
import shutil
from werkzeug.utils import secure_filename
import subprocess
import uuid
import sys
from celery_server import tasks

UPLOAD_FOLDER = '/data/'
ALLOWED_EXTENSIONS = set(['vcf'])
UPLOADED_FILES=set()
JOBS=dict()
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

@app.route('/clear', methods=['GET'])
def clear_session():
    if "files" in session and "uuid" in session:
        for x in session["files"]:
            if os.path.isdir(os.path.join(app.config['UPLOAD_FOLDER'],session["uuid"])):
                shutil.rmtree(os.path.join(app.config['UPLOAD_FOLDER'],session["uuid"]))
    session.clear()
    return redirect("/")

    allfiles=session["files"]
    allfiles.remove(filename)
    session["files"]=allfiles
    return redirect("/")


@app.route('/atomize', methods=['POST'])
def atomize():
    session["piv-index"]=request.form.get("piv-index")
    session["piv-on"]=request.form.get("piv-on")
    session["piv-value"]=request.form.get("piv-value")
    session["merge-check"]=request.form.get("merge-check")
    session["filter-check"]=request.form.get("filter-check")
    session["pipeline"]="atomize"
    if session["uuid"] not in JOBS:
        JOBS[session["uuid"]]=[]
    for x in session["files"]:
        JOBS[session["uuid"]].append({"atomizer":tasks.atomizer.delay(os.path.join(app.config['UPLOAD_FOLDER'],session["uuid"],x))})
    return redirect("/wait")

@app.route('/combine', methods=['GET'])
def combine():
    session["pipeline"]="combine"
    files=[os.path.join(app.config['UPLOAD_FOLDER'],session["uuid"],x+".jsonl") for x in session["files"]]
    JOBS[session["uuid"]].append({"combine":tasks.combine.delay(files,os.path.join(app.config['UPLOAD_FOLDER'],session["uuid"],session["uuid"]+".jsonl"))})
    return redirect("/wait")

@app.route('/mucorelate', methods=['GET'])
def mucorelate():
    session["pipeline"]="mucor"
    app.logger.info(session["piv-index"])
    app.logger.info(session["piv-on"])
    app.logger.info(session["piv-value"])
    app.logger.info(os.path.join(app.config['UPLOAD_FOLDER'],session["uuid"],session["uuid"]+".jsonl"))
    app.logger.info(os.path.join(app.config['UPLOAD_FOLDER'],session["uuid"],session["uuid"]+"_piv.tsv"))
    app.logger.info(" ".join([x.strip() for x in session["piv-index"].split(",")]))
    app.logger.info(" ".join([x.strip() for x in session["piv-on"].split(",")]))
    JOBS[session["uuid"]].append(
        {"mucor3":tasks.mucor3.delay(
            os.path.join(app.config['UPLOAD_FOLDER'],session["uuid"],session["uuid"]+".jsonl"),
            session["piv-index"],
            session["piv-on"],
            session["piv-value"],
            os.path.join(app.config['UPLOAD_FOLDER'],session["uuid"],session["uuid"]+"_piv.tsv"),
            )})
    app.logger.info(JOBS[session["uuid"]][-1]["mucor3"].get())
    return redirect("/wait")

@app.route('/wait', methods=['GET'])
def wait():
    status=[{k:v.ready()} for x in JOBS[session["uuid"]] for k,v in x.items()]
    for x in status:
        for k,v in x.items():
            if not v:
                return render_template("wait.html",status=status)
    if session["pipeline"]=="atomize":
        return redirect("/combine")
    elif session["pipeline"]=="combine":
        return redirect("/mucorelate")
    else:
        return redirect("/Result")

@app.route('/Result', methods=['GET', 'POST'])
def get_results():
    if request.method == 'POST':
        pass
    return render_template("result.html")


if __name__ == '__main__':
    app.secret_key="myflaskapp".encode("utf-8")
    app.run(debug=True,host='0.0.0.0')
