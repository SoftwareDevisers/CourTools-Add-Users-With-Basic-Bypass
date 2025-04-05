import pandas as pd
import requests

import urllib3 
# Suppress only the single warning from urllib3.
urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)

# 1. Load your Excel file
#    Change the path to wherever your .xlsx lives
df = pd.read_excel('./upload.xlsx')

# 2. Confirm you really want to go ahead
user_input = input("Proceed with creating free users? (y/n): ")
if user_input.lower() != 'y':
    print("Aborted.")
    exit(0)

error_messages = []

print('Running. Please wait.')
print()

# 3. Iterate over each row in the DataFrame
for idx, row in df.iterrows():
    # Build your URL (local vs prod)
    #url = "http://localhost:8350/service/create-free-user"
    url = "https://api.courexperience.com/service/create-free-user"
    
    # explicitly replace NaN with empty string
    org_id = '' if pd.isna(row['ORGANIZATION_ID']) else row['ORGANIZATION_ID']

    # 4. Construct the payload from your headers
    payload = {
        'email':           str(row['EMAIL']).strip(),
        'first_name':      str(row['FIRST_NAME']).strip(),
        'last_name':       str(row['LAST_NAME']).strip(),
        'language':        str(row['LANGUAGE']).strip(),
        'organization_id': str(org_id).strip()
    }

    # 5. Fire off the request
    try:
        resp = requests.post(
            url,
            verify=False,
            headers={ 'Content-Type': 'application/json; charset=utf-8' },
            json=payload
        )
    except Exception as e:
        # network / other error
        line_no = idx + 2  # +2 because DataFrame idx 0 → Excel row 2
        error_messages.append(f"Row {line_no}: Exception {e}")
        continue

    # 6. Check for non-200 and record failures
    if resp.status_code != 200:
        line_no = idx + 2
        error_messages.append(f"Row {line_no}: HTTP {resp.status_code} – {resp.text}")

# 7. Report
if error_messages:
    print("\nThe following rows failed to create an account:\n")
    for msg in error_messages:
        print(msg)
else:
    print("✅ All accounts created successfully!")

input("\nPress any button to close this window...")