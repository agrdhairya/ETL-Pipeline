# ⚡ QUICK FIX - Python Error

## You're Seeing This Error:
```
Python was not found; run without arguments to install from the Microsoft Store...
```

## Quick Solution:

### 1️⃣ Install Python
- Go to: https://www.python.org/downloads/
- Download and run the installer
- **✓ Check "Add Python to PATH"**
- Complete installation
- **Restart your terminal**

### 2️⃣ Start Flask Server

**Pick ONE option:**

**A) Double-click file (EASIEST)**
```
Double-click: START_SERVER_v2.bat
```

**B) PowerShell command**
```powershell
.\START_SERVER_ENHANCED.ps1
```

**C) Manual (if others don't work)**
```powershell
"C:\Python312\python.exe" app.py
```

### 3️⃣ Open in Browser
```
http://localhost:5000
```

---

## ✅ Verify Python Works

Open a **new** PowerShell:
```powershell
python --version
```

Should show: `Python 3.12.x` (or similar)

If still seeing Microsoft Store error, follow the full guide: **PYTHON_NOT_FOUND_FIX.md**

---

## 📚 More Details

- **Detailed guide:** PYTHON_NOT_FOUND_FIX.md
- **Installation help:** INSTALL_PYTHON.md
- **Full walkthrough:** START_HERE.md
