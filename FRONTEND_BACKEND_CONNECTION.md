# 🔗 Frontend & Backend Connection Guide

## Current Status

**Before**: Frontend (index.html) ❌ NOT connected to Backend

**After**: Frontend (index.html) ✅ FULLY connected to Backend (Flask API)

---

## 🎯 Two Options Available

### **Option A: Web Interface (Recommended for Demo)**

Use the **Flask web server** with `index.html`:

```bash
# 1. Install packages (includes Flask now)
pip install -r requirement.txt

# 2. Run the web server
python app.py

# 3. Browser opens automatically at http://localhost:5000
# 4. Upload file or paste text
# 5. See results instantly in the web UI
```

**Pros:**
- ✅ Beautiful web interface
- ✅ Drag-and-drop file upload
- ✅ Live results display
- ✅ CSV export button
- ✅ Professional looking
- ✅ Good for judges/presentations

**Cons:**
- Requires Flask running

---

### **Option B: Command Line (Original)**

Use the **simple CLI** with `main.py`:

```bash
# 1. Install packages
pip install -r requirement.txt

# 2. Run the CLI
python main.py

# 3. Choose option from menu
# 4. Wait for processing
# 5. Check outputs/ folder
```

**Pros:**
- ✅ No web server needed
- ✅ Very simple
- ✅ Good for batch processing

**Cons:**
- No visual interface
- Not as impressive for demos

---

## 🚀 How They're Connected Now

### Architecture

```
┌─────────────────────────┐
│    index.html           │
│   (Web Frontend)        │
└────────────┬────────────┘
             │
             │ HTTP Requests
             ▼
    ┌──────────────────┐
    │   app.py (Flask) │
    │   Web Server     │
    └────────┬─────────┘
             │
             │ Processes data
             ▼
    ┌──────────────────┐
    │ etl_pipeline.py  │
    │  (ETL Engine)    │
    └────────┬─────────┘
             │
             │ Saves results
             ▼
    ┌──────────────────┐
    │   outputs/       │
    │   (CSV, JSON)    │
    └──────────────────┘
```

### API Endpoints (Now Available)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/health` | GET | Check if server is running |
| `/process` | POST | Process text data (from textarea) |
| `/upload` | POST | Upload and process file |
| `/download/csv` | GET | Download CSV results |
| `/download/schema` | GET | Download schema JSON |
| `/schema` | GET | View schema as JSON |
| `/preview` | GET | Get data preview |
| `/clear` | POST | Clear all data |

---

## 📝 What Changed in app.py

### New `/process` Endpoint

This is what the frontend now uses:

```python
@app.route('/process', methods=['POST'])
def process_data():
    # Receives raw text from textarea
    # Processes through ETL pipeline
    # Returns JSON with results
    # Frontend displays in table
```

**How it works:**

1. Frontend sends text via POST to `/process`
2. Backend creates temp file
3. ETL pipeline processes it
4. Returns JSON: `{data: [...], summary: {...}}`
5. Frontend displays as table
6. User can download as CSV

---

## ✅ Quick Start (Web Mode)

### Step 1: Update Requirements
```bash
# New packages added: Flask, CORS, Werkzeug
pip install -r requirement.txt
```

### Step 2: Run Flask Server
```bash
python app.py
```

You'll see:
```
🚀 Starting ETL Pipeline Web Server...
📂 Upload folder: inputs
📂 Output folder: outputs
🌐 Frontend will be available at: http://localhost:5000
📡 API endpoints:
  POST   /process        - Process text data
  POST   /upload         - Upload file
  GET    /download/csv   - Download cleaned CSV
  ...
```

### Step 3: Use the Web Interface
- Browser opens automatically
- Upload file OR paste text
- Click "Process Data"
- See results in table
- Click "Download as CSV"

**DONE!** ✅

---

## 🔄 How Frontend & Backend Communicate

### User pastes text and clicks "Process Data"

```javascript
// Frontend (index.html)
const response = await fetch('http://127.0.0.1:5000/process', {
    method: 'POST',
    headers: { 'Content-Type': 'text/plain' },
    body: rawContent  // User's pasted text
});
```

### Backend receives and processes

```python
# Backend (app.py)
@app.route('/process', methods=['POST'])
def process_data():
    content = request.get_data(as_text=True)  # Get text
    
    # Save to temp file
    # Run ETL pipeline
    df, schema = pipeline.run(temp_filename)
    
    # Convert to JSON
    data = df.to_dict('records')
    
    # Return to frontend
    return jsonify({'success': True, 'data': data})
```

### Frontend receives and displays

```javascript
// Get JSON response
const data = await response.json();

// Display in table
displayTable(data.data);
```

---

## 📊 File Relationships

```
index.html  ←→  app.py  ←→  etl_pipeline.py
(Frontend)      (API)        (Engine)
  •Upload       •Routes      •Extract
  •Paste text   •CORS        •Transform
  •Display      •Process     •Load
  •Download     •Download
```

---

## 🎯 Which Option to Use?

### Use **Option A (Web)** if:
- ✅ Presenting to judges
- ✅ Want beautiful UI
- ✅ Need file upload
- ✅ Want live results
- ✅ Need CSV export

### Use **Option B (CLI)** if:
- ✅ Just testing backend
- ✅ Batch processing many files
- ✅ Don't need web interface
- ✅ Prefer command line

---

## ⚡ Complete Workflow (Web Mode)

```
1. pip install -r requirement.txt
   └─ Installs Flask, CORS, pandas, etc.

2. python app.py
   └─ Starts Flask server on :5000

3. Browser opens http://localhost:5000
   └─ Shows index.html

4. User uploads file OR pastes text
   └─ Frontend shows file loaded

5. Click "Process Data" button
   └─ Frontend sends POST to /process

6. Backend processes (1-5 seconds)
   └─ ETL pipeline extracts/transforms/loads

7. Frontend receives JSON response
   └─ Displays results as table

8. User can:
   └─ Download CSV
   └─ Download schema
   └─ Clear and start over
```

---

## 🐛 Troubleshooting

### "Failed to connect to backend"
**Solution:** Make sure Flask is running:
```bash
python app.py
# Should say "Running on http://127.0.0.1:5000"
```

### "ModuleNotFoundError: No module named 'flask'"
**Solution:** Install requirements:
```bash
pip install -r requirement.txt
```

### Port 5000 already in use
**Solution:** Edit app.py last line to use different port:
```python
app.run(debug=True, port=5001)  # Use 5001 instead
```

### Browser doesn't open automatically
**Solution:** Manually open:
```
http://localhost:5000
```

---

## 📚 Files & Their Roles

| File | Role | When Used |
|------|------|-----------|
| `index.html` | Web Frontend | Option A: Web interface |
| `app.py` | Flask API Server | Option A: Handles HTTP requests |
| `main.py` | CLI Menu | Option B: Command line |
| `etl_pipeline.py` | ETL Engine | Both: Core processing |
| `requirement.txt` | Dependencies | Both: Installation |

---

## ✨ Key Improvements Made

✅ **Added `/process` endpoint** - Processes pasted text  
✅ **Added CORS support** - Frontend can talk to backend  
✅ **Added Flask to requirements** - Easy setup  
✅ **Temporary file handling** - Doesn't clutter inputs/  
✅ **Error handling** - Clear error messages  
✅ **JSON responses** - Frontend can display results  

---

## 🎉 Summary

**Now You Have:**

1. ✅ **Frontend (index.html)** - Beautiful web interface
2. ✅ **Backend (app.py)** - Flask API server  
3. ✅ **Connection** - HTTP API between them
4. ✅ **Options** - Choose web OR CLI

**To use:**
```bash
pip install -r requirement.txt
python app.py
```

**Result:** Professional ETL pipeline with web interface! 🚀

---

## 🔗 Next Steps

### Option 1: Try Web Interface
```bash
python app.py
# Visit http://localhost:5000
# Upload a file or paste text
# See results!
```

### Option 2: Stick with CLI
```bash
python main.py
# Choose from menu
```

**Both work perfectly!** Choose what you need. 😊
