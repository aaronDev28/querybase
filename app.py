from flask import Flask, render_template, request, redirect, url_for, send_file, session
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
from sqlalchemy import text, inspect
from io import BytesIO
from socket import gethostname

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.secret_key = 'aaron'
db = SQLAlchemy(app)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return redirect(request.url)
    file = request.files['file']
    if file.filename == '':
        return redirect(request.url)
    if file and file.filename.endswith('.csv'):
        # Clear the existing data in the database
        db.session.execute(text('DELETE FROM data'))
        db.session.commit()
        df = pd.read_csv(file)
        df.to_sql(name='data', con=db.engine, if_exists='replace', index=False)
        return redirect(url_for('query_page'))
    else:
        return "<script>alert('Please upload a CSV file');</script>"

@app.route('/query')
def query_page():
    # Get column names and data types from the database
    inspector = inspect(db.engine)
    columns = inspector.get_columns('data')  # Assuming 'data' is the name of your table
    column_info = [(column['name'], column['type']) for column in columns]

    return render_template('query.html', column_info=column_info)


@app.route('/execute_query', methods=['POST'])
def execute_query():
    query = text(request.form['query'].strip())
    with db.engine.connect() as con:
        result = con.execute(query)
        column_names = result.keys()
        result_data = [dict(zip(column_names, row)) for row in result]

    if not result_data:
        message = "No Rows."
        return render_template('query_result.html', message=message)
    else:
        session['result_data'] = result_data
        return render_template('query_result.html', result_data=result_data)


@app.route('/download_csv', methods=['POST'])
def download_csv():
    result_data = session.get('result_data', None)

    if not result_data:
        return "No data to download."
    else:
        # Create a DataFrame from the query results
        df = pd.DataFrame(result_data)
        # Set up response headers for CSV download
        csv_data = df.to_csv(index=False)

        csv_buffer = BytesIO()
        csv_buffer.write(csv_data.encode())
        csv_buffer.seek(0)

        return send_file(
            csv_buffer,
            mimetype='text/csv',
            as_attachment=True,
            download_name='data.csv'
        )

if __name__ == '__main__':
    db.create_all()
    if 'liveconsole' not in gethostname():
        app.run()
