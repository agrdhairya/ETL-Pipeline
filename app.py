#!/usr/bin/env python3
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from flask import Flask, request, jsonify
from flask_cors import CORS
import os, tempfile
from pathlib import Path

app = Flask(__name__)
CORS(app)
Path('inputs').mkdir(exist_ok=True)
Path('outputs').mkdir(exist_ok=True)

@app.route('/', methods=['GET'])
def index():
    with open('index.html', encoding='utf-8') as f:
        return f.read(), 200, {'Content-Type': 'text/html; charset=utf-8'}

@app.route('/process', methods=['POST'])
def process():
    try:
        from etl_pipeline import ETLPipeline
        
        content = request.get_data(as_text=True)
        if not content.strip():
            return jsonify({'error': 'No data'}), 400
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', dir='inputs', delete=False, encoding='utf-8') as f:
            f.write(content)
            fname = os.path.basename(f.name)
        
        pipeline = ETLPipeline(input_dir='inputs', output_dir='outputs')
        df, schema = pipeline.run(fname)
        
        df = df.fillna('')
        for col in df.columns:
            df[col] = df[col].astype(str)
        
        data = df.to_dict('records')
        
        try:
            os.remove(os.path.join('inputs', fname))
        except:
            pass
        
        return jsonify({'success': True, 'data': data}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("Server starting...", flush=True)
    app.run(host='127.0.0.1', port=5000, debug=False)
