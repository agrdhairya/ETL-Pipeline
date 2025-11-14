# ⚡ QUICK FIX - Server Connection Error

## You Saw This Error?
```
This site can't be reached
localhost refused to connect
ERR_CONNECTION_REFUSED
```

## ✅ Fix in 10 Seconds

### Windows Users (EASIEST!)

1. Open folder: `d:\ETL Pipeline\ETL-Pipeline`
2. Double-click: **`START_SERVER.bat`**
3. Wait 5 seconds
4. Browser opens at `http://localhost:5000`
5. ✅ **DONE!**

### Mac/Linux Users

```bash
cd "d:/ETL Pipeline/ETL-Pipeline"
python app.py
```

Then open browser: `http://localhost:5000`

---

## What Happens Next

```
Browser opens
    ↓
You see ETL Pipeline interface
    ↓
Upload file or paste text
    ↓
Click "Process Data"
    ↓
See results!
```

---

## If It Still Doesn't Work

**Problem:** `Python not found`  
**Fix:** Install Python from https://www.python.org/downloads/

**Problem:** `Flask not found`  
**Fix:** The START_SERVER.bat will install it automatically

**Problem:** `Port 5000 in use`  
**Fix:** Close other applications or edit app.py last line:
```python
app.run(debug=True, port=5001)
```

---

## That's It!

Just **double-click START_SERVER.bat** and you're done! 🚀

For detailed guide: See `HOW_TO_START_SERVER.md`
