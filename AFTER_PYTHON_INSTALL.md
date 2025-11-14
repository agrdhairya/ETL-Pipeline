# After Installing Python - Next Steps

## ✅ Python Installation Complete

Congratulations! Python is now installed on your system.

Now let's verify it's working, then start your ETL pipeline.

---

## 1️⃣ Verify Python Installation

Open a **new** PowerShell window (important - new window so PATH is updated):

```powershell
python --version
```

**Expected output:**
```
Python 3.12.x
```

If you still see the Microsoft Store error:
- Close ALL terminals and PowerShell windows
- Wait 10 seconds
- Open a new PowerShell
- Try again

---

## 2️⃣ Start the Flask Server

You have 3 options. Pick one:

### **Option A: Double-click Batch File (EASIEST)**

1. Open Windows File Explorer
2. Navigate to: `d:\ETL Pipeline\ETL-Pipeline\`
3. Find: `START_SERVER_v2.bat`
4. Double-click it

**What you'll see:**
```
🔍 Checking for Python installation...
✅ Found Python at: C:\Python312\python.exe

📦 Checking dependencies...
✅ All dependencies are installed

🚀 Starting Flask server...
🌐 Web Interface: http://localhost:5000
📡 API Ready for requests
```

---

### **Option B: PowerShell Command**

Open PowerShell and run:

```powershell
cd "d:\ETL Pipeline\ETL-Pipeline"
.\START_SERVER_ENHANCED.ps1
```

---

### **Option C: Direct Python Command**

```powershell
cd "d:\ETL Pipeline\ETL-Pipeline"
python app.py
```

You should see:
```
 * Running on http://127.0.0.1:5000
 * Press CTRL+C to quit
```

---

## 3️⃣ Open in Browser

Once the Flask server is running, open your browser and go to:

```
http://localhost:5000
```

You should see your ETL Pipeline web interface with:
- File upload area (drag & drop)
- Text paste area
- Process Data button
- Empty results table

---

## 🧪 Test It Out

### **Test 1: Paste Text**

1. Click in the text area
2. Paste or type some text (any content is fine):
   ```
   Hello world
   This is test data
   It will be processed
   ```
3. Click "Process Data"
4. You should see results in the table

### **Test 2: Upload Sample File**

1. Drag `sample_data.txt` into the upload area
2. Or use "Choose File" button
3. Click "Process Data"
4. Results should show with extracted data

### **Test 3: Download Results**

1. After processing, click "Download as CSV"
2. File `cleaned_output.csv` should download
3. Open it in Excel or text editor
4. You'll see cleaned, structured data

---

## 📂 Output Files

After processing, check the `outputs/` folder:

```
outputs/
├── cleaned_output.csv       ← Your cleaned data (open in Excel)
├── dynamic_schema.json      ← Column definitions
└── processing_metadata.json ← Processing statistics
```

---

## 🎚️ Using the Web Interface

### Upload Files
- Click the upload area or drag files
- Supports: .txt, .html, .json, .csv, or any text format
- Or paste directly into text box

### Process Data
- Click "Process Data" button
- Pipeline will:
  1. Read the file
  2. Extract content from mixed formats
  3. Infer schema (auto-detect columns)
  4. Normalize data (make consistent)
  5. Load results

### View Results
- Table shows processed records
- Each row is one record
- Columns are auto-detected from your data

### Download Results
- Click "Download as CSV"
- Use in Excel, Python, database, etc.

---

## 🔄 Advanced: CLI Alternative

If you prefer command line, run:

```powershell
python main.py
```

You'll see a menu:
```
1. Process existing files
2. Watch mode (auto-process new files)
3. Batch process with database
4. Exit
```

Choose an option and follow prompts.

---

## 📚 Documentation

**For more help:**

| Task | File |
|------|------|
| General questions | START_HERE.md |
| Server issues | HOW_TO_START_SERVER.md |
| Quick reference | QUICK_REF.md |
| How it works | EXPLANATION.md |
| All details | COMPLETE_SUMMARY.md |

---

## 🧪 Troubleshooting

### Server won't start
```
Check: Is Python running? (python --version)
Try: Restart PowerShell
Try: Run as Administrator
```

### Port 5000 already in use
```
Change port in app.py line: app.run(port=5001)
Or: Stop other Flask servers
```

### Files not processing
```
Check: File format is supported (text, HTML, JSON)
Check: File encoding is UTF-8
Try: sample_data.txt as test
```

### Can't download results
```
Check: outputs/ folder exists
Check: File permissions
Try: Save to Desktop instead
```

---

## 🎉 Success!

Your ETL Pipeline is now:

✅ Running on http://localhost:5000
✅ Processing mixed-format data
✅ Exporting cleaned CSV files
✅ Storing everything locally (privacy)
✅ Ready for real data

---

## 📞 Need Help?

1. **Server issues?** → Check HOW_TO_START_SERVER.md
2. **Processing questions?** → Check EXPLANATION.md
3. **Still stuck?** → Check PYTHON_NOT_FOUND_FIX.md

---

## 🚀 Next: Start Processing

You're all set! Start uploading files or pasting data and watch the magic happen.

Enjoy your ETL pipeline! 🎉
