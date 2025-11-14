# Quick Reference Guide - ETL Pipeline

## 🚀 One-Minute Start

```bash
# 1. Install packages (one time)
pip install -r requirement.txt

# 2. Put file in inputs/ folder

# 3. Run pipeline
python main.py

# 4. Choose option 1, press Enter

# 5. Open outputs/cleaned_output.csv in Excel
```

## 📁 Folder Structure

```
ETL-Pipeline/
├── inputs/                    ← YOUR FILES GO HERE
│   └── your_file.txt
├── outputs/                   ← RESULTS GO HERE
│   ├── cleaned_output.csv     ← OPEN IN EXCEL
│   ├── dynamic_schema.json
│   └── processing_metadata.json
├── main.py                    ← RUN THIS
└── etl_pipeline.py           ← CORE LOGIC
```

## 🎮 Command Reference

```bash
# Interactive menu
python main.py

# Watch for new files (auto-process)
python main.py watch

# Process existing files
python main.py process

# Process with database
python main.py db
```

## 📊 What Happens

```
Mixed File (HTML+JSON+Text)
         ↓
    [EXTRACT]
    ├── HTML → title, links, text
    ├── JSON → all fields, flattened
    └── Text → paragraphs
         ↓
    [INFER SCHEMA]
    → Find all fields + types
         ↓
    [NORMALIZE]
    → Same columns for all records
         ↓
    [LOAD]
    ├── cleaned_output.csv
    ├── dynamic_schema.json
    └── processing_metadata.json
```

## 📝 Input Examples

### HTML
```html
<html>
  <title>My Title</title>
  <body><p>Content here</p></body>
</html>
```

### JSON
```json
{"name": "John", "age": 30}
{"name": "Jane", "age": 25}
```

### Plain Text
```
This is a paragraph.
Another paragraph here.
```

### Mixed (All in one file)
```
<html>...</html>
{"data": "value"}
Some text here
```

✅ **All work! Mix and match!**

## 📊 Output Explanation

### cleaned_output.csv
A spreadsheet with all data in rows/columns

```csv
type,title,content,name,age
html,My Title,Content here,None,None
json,None,None,John,30
json,None,None,Jane,25
text,None,This is a paragraph,None,None
```

### dynamic_schema.json
What fields exist and their types

```json
{
  "name": {
    "type": ["str"],
    "nullable": true,
    "present_in": 2
  },
  "age": {
    "type": ["int"],
    "nullable": true,
    "present_in": 2
  }
}
```

### processing_metadata.json
Processing statistics

```json
{
  "filename": "data.txt",
  "total_items": 4,
  "items_by_type": {
    "html": 1,
    "json": 2,
    "text": 1
  }
}
```

## 🎯 Usage Scenarios

### Scenario 1: Process Once
```bash
python main.py
# Select option 1
# Done!
```

### Scenario 2: Keep Processing Files
```bash
python main.py watch
# Leave running
# Drop files in inputs/
# They auto-process
```

### Scenario 3: Save to Database
```bash
python main.py db
# Saves to SQLite for queries
```

## 💡 Tips

| Tip | Benefit |
|-----|---------|
| Use watch mode | Auto-process new files |
| Use db mode | Query results with SQL |
| Keep file small | Faster processing |
| Mixed formats | Just works™ |
| Excel can't handle? | Use dynamic_schema.json |

## ❓ FAQ

**Q: Can I process .html files?**  
A: Yes! Any text-based file works.

**Q: Will my data be sent anywhere?**  
A: No! Everything is local only.

**Q: Can I see what it extracts?**  
A: Yes! Check processed_metadata.json

**Q: Can I use the cleaned CSV in Excel?**  
A: Yes! That's exactly what it's for.

**Q: What if my file is large?**  
A: Works fine! Just takes a bit longer.

**Q: Can I modify the extraction logic?**  
A: Yes! Edit etl_pipeline.py (well-commented)

## 🔧 Troubleshooting

| Error | Fix |
|-------|-----|
| `ModuleNotFoundError: pandas` | Run `pip install -r requirement.txt` |
| `No files found` | Put a file in inputs/ folder |
| `Permission denied` | Make sure inputs/ and outputs/ exist |
| Garbled text | File encoding issue (auto-handled) |

## 📈 Performance Tips

- Process files < 10MB for instant results
- Use watch mode to avoid manual runs
- Use database mode for multiple analyses
- Split huge files into smaller chunks

## 🎓 Learning Path

1. Read this file first (✓ you're here)
2. Run with sample_data.txt
3. Check outputs/cleaned_output.csv
4. Try your own file
5. Read README.md for details
6. Explore etl_pipeline.py code

## ✨ Key Features Checklist

- ✅ Detects HTML automatically
- ✅ Parses JSON objects
- ✅ Extracts plain text
- ✅ Creates dynamic schema
- ✅ Normalizes all data
- ✅ Saves as CSV
- ✅ Saves metadata
- ✅ Supports watch mode
- ✅ Optional SQLite
- ✅ Local storage only
- ✅ No APIs
- ✅ Beginner-friendly

## 🚀 Getting Started Right Now

```bash
# Copy your file to inputs/
cp your_data.txt inputs/

# Run the pipeline
python main.py

# Press 1 and hit Enter

# Wait 1-5 seconds...

# Open: outputs/cleaned_output.csv
```

Done! Your data is now structured. 🎉

---

**Questions?** Check:
- README.md (detailed)
- SETUP.md (quick setup)
- TEST_DEMO.py (see it in action)
- EXPLANATION.md (how it works)
