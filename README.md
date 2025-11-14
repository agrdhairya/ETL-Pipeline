# Simple ETL Pipeline 🚀

A straightforward, beginner-friendly ETL (Extract, Transform, Load) pipeline that processes unstructured mixed-format files (HTML, JSON, Text, Base64) and outputs clean, structured data.

## 🎯 Architecture

```
INPUT FILES (inputs/)
    ↓
ETL PIPELINE
├── Extract: Detect and extract HTML, JSON, Text, Base64
├── Transform: Infer schema and normalize data
└── Load: Save as CSV, JSON schema, and metadata
    ↓
OUTPUT FILES (outputs/)
├── cleaned_output.csv
├── dynamic_schema.json
├── processing_metadata.json
└── etl_data.db (optional with --db flag)
```

## 📋 Features

✅ **Simple File Processing** - No APIs, just pure Python  
✅ **Mixed Format Detection** - Handles HTML, JSON, Text, Base64 in one file  
✅ **Dynamic Schema Inference** - Auto-detects all data types and creates schema  
✅ **Local Storage Only** - All data stored locally (privacy first)  
✅ **Watch Mode** - Auto-process new files as they're added  
✅ **Optional SQLite** - Store data in local database if needed  
✅ **Beginner Friendly** - Clear, well-commented code

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirement.txt
```

Required packages:
- `pandas` - Data processing
- `beautifulsoup4` - HTML parsing
- `lxml` - XML/HTML support
- `watchdog` - File monitoring

### 2. Prepare Input File

Place your mixed-format file in the `inputs/` folder. For example:

```
inputs/
└── raw_data.txt  (can contain HTML, JSON, plain text, base64)
```

### 3. Run the Pipeline

**Option 1: Interactive Menu**
```bash
python main.py
```

**Option 2: Watch and Auto-Process**
```bash
python main.py watch
```
Drop files into `inputs/` and they'll be processed automatically!

**Option 3: Process Existing Files**
```bash
python main.py process
```

**Option 4: With Database Storage**
```bash
python main.py db
```

## 📂 Directory Structure

```
ETL-Pipeline/
├── main.py                    # Main entry point (simple CLI)
├── etl_pipeline.py           # Core ETL logic
├── requirement.txt           # Dependencies
├── index.html                # Frontend (for reference)
│
├── inputs/                   # Your input files go here
│   └── raw_scraped_file.txt
│
└── outputs/                  # Processed results
    ├── cleaned_output.csv              # Structured data
    ├── dynamic_schema.json             # Auto-detected schema
    ├── processing_metadata.json        # Processing info
    └── etl_data.db                    # SQLite DB (optional)
```

## 📝 Input File Format

Your input file can be a mix of any of these formats:

### Example Mixed File:

```html
<html>
  <head><title>Sample Data</title></head>
  <body>
    <p>This is some plain text content extracted from HTML.</p>
  </body>
</html>

{"user": "john", "email": "john@example.com", "age": 28}
{"product": "laptop", "price": 999.99, "stock": 5}

Here is a plain text paragraph that should be captured.
Another paragraph with important information goes here.

data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==
```

The pipeline will:
1. Extract all HTML blocks
2. Parse JSON objects
3. Capture plain text paragraphs
4. Store media metadata

## 📊 Output Files

### `cleaned_output.csv`
All extracted data in structured table format with consistent columns.

### `dynamic_schema.json`
Auto-inferred schema showing:
- Data types detected
- Which fields are nullable
- How many records have each field

Example:
```json
{
  "type": {
    "type": ["str"],
    "nullable": false,
    "present_in": 10
  },
  "user": {
    "type": ["str"],
    "nullable": true,
    "present_in": 3
  }
}
```

### `processing_metadata.json`
Processing statistics:
```json
{
  "filename": "raw_data.txt",
  "start_time": "2025-11-14T10:30:45.123456",
  "end_time": "2025-11-14T10:30:47.654321",
  "total_items": 15,
  "items_by_type": {
    "html": 2,
    "json": 5,
    "text": 8
  }
}
```

## 🔄 How It Works

### Step 1: READ
Reads the entire file, handling UTF-8 and Latin-1 encodings.

### Step 2: EXTRACT
Detects and extracts:
- **HTML blocks** → Extracts title, text, links, images
- **JSON objects** → Parses and flattens nested structures
- **Plain text** → Groups into paragraphs
- **Base64 data** → Captures as media metadata

### Step 3: SCHEMA INFERENCE
- Collects all unique keys across all extracted objects
- Detects data types
- Marks nullable fields
- Creates dynamic schema

### Step 4: NORMALIZE
- Fills missing values with None
- Creates consistent DataFrame with all columns
- Ensures every record has same structure

### Step 5: LOAD
- Saves as `cleaned_output.csv`
- Saves schema as `dynamic_schema.json`
- Saves metadata as `processing_metadata.json`
- Optionally stores in SQLite database

## 💾 Privacy & Local Storage

✅ All files stored locally in `inputs/` and `outputs/` folders  
✅ No data sent to any external servers  
✅ Full control over your data  
✅ Optional SQLite database for structured queries

## 🐛 Example Workflow

```bash
# 1. Place your file
# Copy raw_scraped_file.txt to inputs/ folder

# 2. Run the pipeline
python main.py

# 3. Choose option 1 to process existing files
# Output: 
# ✓ cleaned_output.csv
# ✓ dynamic_schema.json
# ✓ processing_metadata.json

# 4. Check results
# Open outputs/cleaned_output.csv in Excel or text editor
# View outputs/dynamic_schema.json for data structure
```

## 🎓 For Beginners

**What is ETL?**
- **Extract**: Get data from different sources/formats
- **Transform**: Clean and structure the data  
- **Load**: Save it in a usable format

**What this pipeline does:**
1. Reads messy, mixed-format files
2. Finds all the different types of data
3. Extracts useful information from each
4. Puts it all in a clean spreadsheet format
5. Saves the results locally

## 📞 Troubleshooting

**Problem**: "No files found in inputs/"  
**Solution**: Make sure you've placed a file in the `inputs/` folder

**Problem**: UnicodeDecodeError  
**Solution**: The pipeline automatically tries UTF-8 and Latin-1. Most files work fine.

**Problem**: "Invalid JSON" warnings  
**Solution**: Normal - not all curly braces `{}` are valid JSON. The pipeline skips invalid ones.

## 🔧 Advanced Usage

### With Database (SQLite)

```bash
python main.py db
```

This creates `outputs/etl_data.db` with tables:
- `processed_data` - All extracted records
- `schemas` - Schema information for each run

### Watch Mode (Auto-Process)

```bash
python main.py watch
```

Files added to `inputs/` are automatically processed!

### Programmatic Usage

```python
from etl_pipeline import ETLPipeline

pipeline = ETLPipeline(input_dir="inputs", output_dir="outputs", use_db=True)
df, schema = pipeline.run("myfile.txt")

print(f"Processed {len(df)} records")
print(f"Schema fields: {len(schema)}")
```

## 📋 Sample Input File

See `sample_data.txt` for an example of mixed-format input.

## 📄 License

MIT License - Use freely!

## ✅ Ready to Use!

Just place your file in `inputs/` and run:

```bash
python main.py
```

Choose option 1 and watch it process! 🎉
