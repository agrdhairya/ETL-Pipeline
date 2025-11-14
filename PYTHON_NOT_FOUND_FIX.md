# 🚨 CRITICAL: Python Not Installed - Solution

## The Problem

Your system only has a **Microsoft Store Python alias** (a shortcut) but not actual Python.

When you try:
```
python app.py
python3 app.py
```

You get:
```
Python was not found; run without arguments to install from the Microsoft Store...
```

This is because Windows is redirecting the `python` command to the Microsoft Store installer instead of running actual Python.

---

## ✅ Solution (3 Steps)

### Step 1: Install Python

**Visit:** https://www.python.org/downloads/

1. Click the large **yellow button** that says "Download Python 3.12" (or any 3.10+)
2. Run the `.exe` file
3. **IMPORTANT:** Check the box for **"Add Python to PATH"** at the bottom of the installer
4. Click "Install Now"
5. Wait for installation to complete

### Step 2: Close All Terminals

- Close PowerShell
- Close Command Prompt
- Close VS Code (if it has terminals open)
- Close any other terminal windows

### Step 3: Try One of These Options

Pick whichever option is easiest for you:

#### **Option A: Double-click START_SERVER_v2.bat** (EASIEST)
1. Open Windows File Explorer
2. Navigate to: `d:\ETL Pipeline\ETL-Pipeline`
3. Find `START_SERVER_v2.bat`
4. Double-click it

Expected result:
- A window will appear saying "Found Python at: ..."
- Then "Starting Flask server..."
- Finally: "Web Interface: http://localhost:5000"

#### **Option B: Run PowerShell script**
1. Open PowerShell
2. Navigate to your project:
   ```powershell
   cd "d:\ETL Pipeline\ETL-Pipeline"
   ```
3. Run:
   ```powershell
   .\START_SERVER_ENHANCED.ps1
   ```

#### **Option C: Manual command (if others fail)**
1. Find where Python is installed:
   - Usually: `C:\Python312\` or `C:\Program Files\Python312\`
2. Open PowerShell and run:
   ```powershell
   "C:\Python312\python.exe" app.py
   ```
   (Replace with your actual Python path)

---

## 🎯 Once Server Starts

You'll see something like:
```
🚀 Starting Flask server...

🌐 Web Interface: http://localhost:5000
📡 API Ready for requests
```

Then:
1. Open your browser
2. Go to: **http://localhost:5000**
3. You should see the ETL Pipeline web interface

---

## 🧪 Verify Python Installation Works

After installing, open a **new** PowerShell and type:
```powershell
python --version
```

You should see:
```
Python 3.12.x
```

If you still see the Microsoft Store message, Python wasn't installed correctly. Try reinstalling.

---

## ❓ Still Not Working?

### Check 1: Did Python installation complete?
- Look for: `C:\Python312\` or `C:\Program Files\Python312\`
- If not there, reinstall Python

### Check 2: Did you restart after installing Python?
- You MUST close all terminals and open new ones
- The PATH doesn't update in already-open terminals

### Check 3: Did you check "Add Python to PATH"?
- If you missed this during installation, you need to reinstall
- Make sure this checkbox is checked

### Check 4: Microsoft Store Alias Interfering
Only needed if Python is installed but still getting the error:

1. Open **Settings** on Windows
2. Go to **Apps** → **App aliases**
3. Find **python.exe** and **python3.exe**
4. Toggle them **OFF**
5. Restart PowerShell
6. Try again

---

## 📝 Installation Checklist

Before asking for help, verify:

- ✅ Downloaded Python from https://www.python.org/downloads/
- ✅ Ran the installer (.exe file)
- ✅ **Checked "Add Python to PATH"** during installation
- ✅ Installation completed without errors
- ✅ Restarted all terminal windows
- ✅ New terminal shows: `python --version` → `Python 3.12.x`
- ✅ Navigated to ETL Pipeline folder
- ✅ Tried one of the server start options

---

## 📦 Dependencies Installation

Once Python works, the server starter scripts will automatically:
1. Check if Flask and other packages are installed
2. If missing, run: `pip install -r requirement.txt`
3. Install all 7 required packages

**Packages installed:**
- pandas (data processing)
- beautifulsoup4 (HTML parsing)
- lxml (XML support)
- watchdog (file monitoring)
- flask (web server)
- flask-cors (cross-origin requests)
- werkzeug (WSGI utilities)

---

## 🎉 Next Steps

Once the Flask server is running:

1. **Web Interface:** http://localhost:5000
   - Drag and drop files
   - Paste text data
   - Click "Process Data"
   - Download results as CSV

2. **Alternative - CLI:** If you prefer command line:
   ```powershell
   python main.py
   ```

3. **Check Output:** Results are saved in:
   - `outputs/cleaned_output.csv`
   - `outputs/dynamic_schema.json`
   - `outputs/processing_metadata.json`

---

## 🆘 Need More Help?

Check these files:
- **START_HERE.md** - Full walkthrough
- **HOW_TO_START_SERVER.md** - Server startup options
- **INSTALL_PYTHON.md** - Detailed Python installation guide
- **QUICK_FIX.md** - Quick reference
