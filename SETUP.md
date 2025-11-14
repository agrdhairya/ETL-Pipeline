# ⚡ ETL Pipeline - Setup & Getting Started

## 🎯 What You Have

A simple, no-frills ETL pipeline that:
- Reads messy files with mixed HTML, JSON, and text
- Automatically detects and extracts data
- Creates a clean spreadsheet (CSV)
- Saves everything locally (privacy-first)

## 🚀 5-Minute Setup

### Step 1: Install Python Packages
```bash
pip install -r requirement.txt
```

### Step 2: Create Input Folder (if not exists)
```bash
mkdir inputs
mkdir outputs
```

### Step 3: Add Your File
Place your file in the `inputs/` folder:
```
inputs/
└── your_file.txt (or .html, .json, etc.)
```

### Step 4: Run the Pipeline
```bash
python main.py
```

You'll see:
```
🚀 SIMPLE ETL PIPELINE
=======================================================

Options:
1. Process existing files in inputs/ folder
2. Watch inputs/ folder for new files (auto-process)
3. Process a specific file
4. View outputs
5. Exit
-------------------------------------------------------

Enter your choice (1-5): 1
```

Press `1` and hit Enter.

### Step 5: Check Results
Your processed data is in `outputs/`:
- `cleaned_output.csv` ← Open this in Excel
- `dynamic_schema.json` ← Schema info
- `processing_metadata.json` ← Processing stats

## 📝 What Kind of Files to Process?

Your input file should contain any mix of:

**HTML:**
```html
<html>
  <body>
    <p>Some content here</p>
  </body>
</html>
```

**JSON:**
```json
{"name": "John", "age": 30}
{"name": "Jane", "age": 25}
```

**Plain Text:**
```
This is a paragraph of plain text.
It will be extracted and included.
```

**All in one file?** ✅ Works perfectly!

## 🎮 Different Ways to Use It

### Interactive Menu (Default)
```bash
python main.py
```

### Watch Mode (Auto-Process)
Add files to `inputs/` and they process automatically:
```bash
python main.py watch
```

### Process Existing Files
Process everything in `inputs/`:
```bash
python main.py process
```

### With Database
Save results to SQLite database:
```bash
python main.py db
```

## 📊 Understanding the Output

### cleaned_output.csv
A spreadsheet with all your extracted data in rows and columns.

### dynamic_schema.json
Shows what data types were found:
```json
{
  "name": {"type": ["str"], "nullable": false, "present_in": 5},
  "age": {"type": ["int"], "nullable": true, "present_in": 3}
}
```

### processing_metadata.json
Shows what was processed:
```json
{
  "filename": "data.txt",
  "total_items": 25,
  "items_by_type": {"html": 1, "json": 15, "text": 9}
}
```

## 💡 Tips

✅ **Tip 1**: Your data stays local - never sent anywhere  
✅ **Tip 2**: Use watch mode for continuous processing  
✅ **Tip 3**: Output files keep getting replaced, so download them if you want to keep them  
✅ **Tip 4**: File encoding? No problem - handles UTF-8 and Latin-1

## ❓ Quick Questions

**Q: Will my data be private?**  
A: Yes! Everything runs locally on your machine.

**Q: What file types work?**  
A: Any text file - .txt, .html, .json, .csv, .log, etc.

**Q: Can I have HTML, JSON, and text all in one file?**  
A: Yes! That's exactly what it's designed for.

**Q: Do I need to install anything else?**  
A: Just the packages in `requirement.txt`. No databases or servers needed.

**Q: Can I view the results in Excel?**  
A: Yes! Open `outputs/cleaned_output.csv` in Excel or any spreadsheet app.

## 🎉 You're Ready!

```bash
python main.py
```

That's it! Choose option 1 and watch your file get processed. 

See `README.md` for more detailed documentation.
