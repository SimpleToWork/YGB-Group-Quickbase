@echo off
pip install virtualenv

cd /d "C:\Users\%USERNAME%\Desktop\New Projects\YGB Group\YGB-Group-Quickbase"
python -m virtualenv venv

cd venv/scripts
call activate.bat

cd /d "C:\Users\%USERNAME%\Desktop\New Projects\YGB Group\YGB-Group-Quickbase"
pip install -r requirements.txt

cmd /k