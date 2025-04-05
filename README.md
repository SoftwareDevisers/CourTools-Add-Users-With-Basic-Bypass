1. To use this add data to the upload.xlsx spreadsheet. Below is more information for the fields.

2. Once you have added all your data to that file, make sure to save it.

3. Double click the run_tool.exe file for the program to run.

Required fields:
    * EMAIL	
    * FIRST_NAME	
    * LAST_NAME	
    * LANGUAGE	(Can either be en or es) -> en for English, es for Spanish
    
Non required fields:
    * ORGANIZATION_ID (This only matters if we want to associate a user with a company) -> Will be rarely used most likely

--------------------------------------------------------------------------------------------------------------------------
DEVELOPER BUILD INFO:

pyinstaller --onefile --name Add User Tool main.py