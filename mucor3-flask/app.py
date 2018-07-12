from flask import Flask, flash, request, redirect, url_for
import os
from werkzeug.utils import secure_filename
import subprocess
import uuid
UPLOAD_FOLDER = '/tmp/'
ALLOWED_EXTENSIONS = set(['vcf'])
UPLOADED_FILES=[]
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
                tmp_file=os.path.join(app.config['UPLOAD_FOLDER'], str(uuid.uuid4())+"."+".".join(filename.split(".")[1:]))
                UPLOADED_FILES.append(tmp_file)
                file.save(tmp_file)
                out =open(tmp_file+".jsonl","w")
                print(subprocess.call(['../vcf_atomizer/bin/atomizer',tmp_file],stdout=out))
                return redirect('/')
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=files multiple>
      <input type=submit value=Upload>
    </form>
    '''

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')
