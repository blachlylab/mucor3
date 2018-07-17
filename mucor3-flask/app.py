from flask import Flask, flash, request, redirect, url_for,render_template
import os
from werkzeug.utils import secure_filename
import subprocess
import uuid
UPLOAD_FOLDER = '/tmp/'
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
        files = request.files.getlist('files')
        # if user does not select file, browser also
        # submit an empty part without filename
        for file in files:
            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config['UPLOAD_FOLDER'],filename))
                UPLOADED_FILES.add(file.filename)
    return render_template("upload.html",files=sorted(UPLOADED_FILES))

@app.route('/mucorelate', methods=['POST'])
def mucorelate():
    return request.form.get("piv-index")

@app.route('/Result', methods=['GET', 'POST'])
def get_results():
    if request.method == 'POST':
        pass
    return render_template("result.html",files=UPLOADED_FILES)


if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')
