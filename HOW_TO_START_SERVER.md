# 🚀 How to Start the Flask Web Server

## Problem You're Seeing
```
This site can't be reached
localhost refused to connect
ERR_CONNECTION_REFUSED
```

**Reason:** The Flask server isn't running yet.

---

## Solution: Start the Server

### Option 1: Double-Click Batch File (Easiest!)

```
📁 ETL-Pipeline/
├── START_SERVER.bat  ← Double-click this!
```

**Steps:**
1. Open folder: `d:\ETL Pipeline\ETL-Pipeline`
2. Double-click: `START_SERVER.bat`
3. A terminal window opens
4. Server starts automatically
5. Browser may open at `http://localhost:5000`

**You should see:**
```
🚀 Starting Flask server...
🌐 Web Interface: http://localhost:5000
 * Running on http://127.0.0.1:5000
```

### Option 2: PowerShell Script

```powershell
# Open PowerShell and run:
cd "d:\ETL Pipeline\ETL-Pipeline"
.\START_SERVER.ps1
```

### Option 3: Manual (If Python Works)

```bash
cd "d:\ETL Pipeline\ETL-Pipeline"
pip install -r requirement.txt
python app.py
```

---

## What to Do After Server Starts

1. **Look for output like:**
```
🚀 Starting ETL Pipeline Web Server...
📂 Upload folder: inputs
📂 Output folder: outputs
🌐 Frontend will be available at: http://localhost:5000
📡 API endpoints:
  POST   /process
  POST   /upload
  GET    /download/csv
```

2. **Open browser:**
   - Go to: `http://localhost:5000`
   - Or just click the browser tab that already opened

3. **You should see:**
   - Upload zone with drag & drop
   - Textarea for pasting text
   - "Process Data" button
   - "Download as CSV" button

---

## Troubleshooting

### "Python was not found"
**Solution:** Install Python from https://www.python.org/downloads/
- Make sure to check "Add Python to PATH" during installation
- Restart terminal after installation

### "Module not found: flask"
**Solution:** Install requirements:
```bash
pip install -r requirement.txt
```

### Port 5000 already in use
**Solution:** 
- Close other applications using port 5000
- Or edit app.py last line to use different port:
```python
app.run(debug=True, port=5001)  # Change 5000 to 5001
```

### Browser still shows error
**Solution:**
- Make sure terminal shows "Running on http://127.0.0.1:5000"
- Try refreshing browser (Ctrl+R or Cmd+R)
- Try `http://127.0.0.1:5000` instead of `localhost:5000`

---

## What Server Does

| Component | Function |
|-----------|----------|
| Flask App | Listens on port 5000 |
| `/process` endpoint | Accepts text, processes through ETL |
| `/upload` endpoint | Accepts file upload |
| `index.html` | Frontend served at root |
| ETL Engine | Processes data in background |

---

## Keep Server Running

- Don't close the terminal window
- Server keeps running until you press Ctrl+C
- Can have multiple browser tabs using same server

---

## Next Steps

1. ✅ Double-click `START_SERVER.bat`
2. ✅ Wait for "Running on..." message
3. ✅ Browser opens to `http://localhost:5000`
4. ✅ Upload file or paste text
5. ✅ Click "Process Data"
6. ✅ See results!

---

**That's it!** The server should start now. 🎉
