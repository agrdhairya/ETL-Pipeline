# 🐍 Installing Python - Complete Guide

Your system has a **Microsoft Store Python shortcut** but not the actual Python installation. Here's how to fix it:

## ⚡ Quick Install (Recommended)

### Step 1: Download Python
1. Visit: **https://www.python.org/downloads/**
2. Click the big yellow **"Download Python 3.12.x"** button
   - (or any 3.10+ version will work)
3. Save the installer

### Step 2: Run the Installer
1. **Double-click** the downloaded `.exe` file
2. A setup window will appear
3. **✓ CRITICAL**: Check the box for **"Add Python to PATH"** at the bottom
4. Click **"Install Now"**
5. Wait for installation to complete

### Step 3: Verify Installation
Close your terminal and open a **new PowerShell** window, then type:
```powershell
python --version
```

You should see something like:
```
Python 3.12.x
```

### Step 4: Install Dependencies
In the same PowerShell window, navigate to your ETL Pipeline folder:
```powershell
cd "d:\ETL Pipeline\ETL-Pipeline"
pip install -r requirement.txt
```

### Step 5: Start the Server
Double-click **`START_SERVER_v2.bat`** in your folder

---

## ✅ If You Already Have Python Installed

If Python is installed but not found, you may need to fix the Microsoft Store alias:

### Option A: Disable Microsoft Store Python Alias (Windows 10/11)
1. Open **Settings**
2. Go to **Apps** → **App aliases**
3. Find **python.exe** and **python3.exe**
4. Toggle them **OFF**
5. Restart your terminal
6. Try running Flask server again

### Option B: Use Full Path to Python
If Option A doesn't work, find your Python installation:

**Check common locations:**
- `C:\Python312\`
- `C:\Program Files\Python312\`
- `C:\Users\[YourName]\AppData\Local\Programs\Python\Python312\`

Once found, run:
```powershell
C:\Python312\python.exe app.py
```

---

## 🧪 Testing Your Installation

After installing Python and dependencies:

### Test 1: Python Works
```powershell
python --version
```
Expected output: `Python 3.10.x` or higher

### Test 2: Flask Installed
```powershell
python -c "import flask; print(flask.__version__)"
```
Expected output: `3.0.0` or similar

### Test 3: Other Dependencies
```powershell
python -c "import pandas; print('Pandas:', pandas.__version__)"
python -c "import bs4; print('BeautifulSoup:', bs4.__version__)"
```

---

## 🚀 Now Start Your ETL Pipeline

### Option 1: Web Interface (Easy)
Double-click: **`START_SERVER_v2.bat`**

Then open your browser to: **http://localhost:5000**

### Option 2: Command Line
```powershell
python main.py
```

A menu will appear with options for processing files.

---

## ❓ Troubleshooting

**Q: Still getting "Python was not found"?**
- Close ALL terminals and open a new one
- If using VS Code terminal, restart VS Code
- Reinstall Python with "Add to PATH" checked

**Q: Installer says Python is already installed?**
- Click "Modify" or "Repair"
- Make sure "Add to PATH" is checked

**Q: START_SERVER_v2.bat won't work?**
- Try double-clicking it from Windows File Explorer
- Don't run from terminal/PowerShell directly

**Q: Still having issues?**
- Try the manual method:
  ```powershell
  C:\Python312\python.exe app.py
  ```
- Replace `C:\Python312\` with wherever Python is actually installed

---

## 📦 What Gets Installed

The `requirement.txt` contains these packages:
- **pandas** - Data processing
- **beautifulsoup4** - HTML parsing
- **lxml** - XML/HTML support
- **watchdog** - File monitoring
- **flask** - Web server
- **flask-cors** - Cross-origin requests
- **werkzeug** - WSGI utilities

All are installed automatically with: `pip install -r requirement.txt`

---

## ✨ Success!

Once Python is installed and PATH is set:
- ✅ `python app.py` will start the Flask server
- ✅ `http://localhost:5000` will load your web interface
- ✅ You can upload files or paste data and see results
- ✅ Everything is stored locally on your computer

**Need help?** Check the other markdown files in this folder:
- `START_HERE.md` - Full walkthrough
- `HOW_TO_START_SERVER.md` - Server startup options
- `QUICK_FIX.md` - Quick reference
