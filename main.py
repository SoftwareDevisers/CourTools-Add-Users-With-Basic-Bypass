import pandas as pd
import requests
import urllib3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# Suppress only the single warning from urllib3.
urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)

# Configuration
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
BATCH_SIZE = 10  # Process in batches
DELAY_BETWEEN_REQUESTS = 0.5  # seconds
MAX_WORKERS = 3  # Concurrent workers
TIMEOUT = 30  # Request timeout in seconds

def process_user(row, idx, session):
    """Process a single user with retry logic"""
    # Build URL
    url = "https://api.courexperience.com/service/create-free-user"
    url = "http://localhost:8350/service/create-free-user"

    
    # Explicitly replace NaN with empty string
    org_id = '' if pd.isna(row['ORGANIZATION_ID']) else row['ORGANIZATION_ID']
    
    # Construct the payload
    payload = {
        'email':           str(row['EMAIL']).strip(),
        'first_name':      str(row['FIRST_NAME']).strip(),
        'last_name':       str(row['LAST_NAME']).strip(),
        'language':        str(row['LANGUAGE']).strip().lower(),
        'organization_id': str(org_id).strip()
    }
    
    line_no = idx + 2  # +2 because DataFrame idx 0 → Excel row 2
    
    # Retry logic
    for attempt in range(MAX_RETRIES):
        try:
            # Add delay between requests to avoid overwhelming the server
            if attempt > 0:
                time.sleep(RETRY_DELAY * attempt)
            
            resp = session.post(
                url,
                verify=False,
                headers={'Content-Type': 'application/json; charset=utf-8'},
                json=payload,
                timeout=TIMEOUT
            )
            
            if resp.status_code == 200:
                return {
                    'success': True,
                    'row': line_no,
                    'email': payload['email']
                }
            elif resp.status_code == 504:
                # Gateway timeout - retry
                if attempt < MAX_RETRIES - 1:
                    print(f"Row {line_no}: 504 Gateway Timeout, retrying... (attempt {attempt + 1}/{MAX_RETRIES})")
                    continue
                else:
                    return {
                        'success': False,
                        'row': line_no,
                        'email': payload['email'],
                        'error': f"HTTP 504 - Gateway Timeout after {MAX_RETRIES} attempts"
                    }
            else:
                # Other HTTP errors - don't retry
                return {
                    'success': False,
                    'row': line_no,
                    'email': payload['email'],
                    'error': f"HTTP {resp.status_code} - {resp.text}"
                }
                
        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                print(f"Row {line_no}: Request timeout, retrying... (attempt {attempt + 1}/{MAX_RETRIES})")
                continue
            else:
                return {
                    'success': False,
                    'row': line_no,
                    'email': payload['email'],
                    'error': f"Request timeout after {MAX_RETRIES} attempts"
                }
        except Exception as e:
            return {
                'success': False,
                'row': line_no,
                'email': payload['email'],
                'error': f"Exception: {str(e)}"
            }
    
    return {
        'success': False,
        'row': line_no,
        'email': payload['email'],
        'error': "Unknown error"
    }

def main():
    # 1. Load your Excel file
    try:
        df = pd.read_excel('./upload.xlsx')
        print(f"Loaded {len(df)} records from upload.xlsx")
    except FileNotFoundError:
        print("Error: upload.xlsx not found in current directory")
        input("\nPress Enter to close...")
        return
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        input("\nPress Enter to close...")
        return
    
    # Validate required fields
    required_fields = ['EMAIL', 'FIRST_NAME', 'LAST_NAME', 'LANGUAGE']
    missing_fields = [field for field in required_fields if field not in df.columns]
    if missing_fields:
        print(f"Error: Missing required fields: {missing_fields}")
        input("\nPress Enter to close...")
        return
    
    # 2. Confirm you really want to go ahead
    user_input = input(f"Proceed with creating {len(df)} free users? (y/n): ")
    if user_input.lower() != 'y':
        print("Aborted.")
        return
    
    print('\nRunning. Please wait...')
    print(f'Processing in batches of {BATCH_SIZE} with {MAX_WORKERS} concurrent workers\n')
    
    # Create a session for connection pooling
    session = requests.Session()
    session.verify = False
    
    # Results tracking
    successful_users = []
    failed_users = []
    
    # Process in batches to avoid overwhelming the server
    total_batches = (len(df) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min((batch_num + 1) * BATCH_SIZE, len(df))
        batch_df = df.iloc[start_idx:end_idx]
        
        print(f"Processing batch {batch_num + 1}/{total_batches} (rows {start_idx + 1}-{end_idx})...")
        
        # Process batch with thread pool
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for idx, row in batch_df.iterrows():
                future = executor.submit(process_user, row, idx, session)
                futures.append(future)
                time.sleep(DELAY_BETWEEN_REQUESTS)  # Rate limiting
            
            # Collect results
            for future in as_completed(futures):
                result = future.result()
                if result['success']:
                    successful_users.append(result)
                    print(f"✅ Row {result['row']}: {result['email']} - Success")
                else:
                    failed_users.append(result)
                    print(f"❌ Row {result['row']}: {result['email']} - {result['error']}")
        
        # Delay between batches
        if batch_num < total_batches - 1:
            print(f"Waiting before next batch...")
            time.sleep(2)
    
    # Close the session
    session.close()
    
    # 7. Report
    print("\n" + "="*50)
    print(f"SUMMARY: {len(successful_users)} successful, {len(failed_users)} failed")
    print("="*50)
    
    if failed_users:
        print("\nFailed accounts:")
        for user in failed_users:
            print(f"Row {user['row']}: {user['email']} - {user['error']}")
        
        # Save failed records to a new Excel file for retry
        failed_df = df.iloc[[f['row'] - 2 for f in failed_users]]
        failed_df.to_excel('failed_users.xlsx', index=False)
        print("\nFailed records saved to 'failed_users.xlsx' for retry")
    else:
        print("\n✅ All accounts created successfully!")
    
    # Save detailed log
    with open('migration_log.json', 'w') as f:
        json.dump({
            'successful': successful_users,
            'failed': failed_users,
            'summary': {
                'total': len(df),
                'successful': len(successful_users),
                'failed': len(failed_users)
            }
        }, f, indent=2)
    print("\nDetailed log saved to 'migration_log.json'")
    
    input("\nPress Enter to close...")

if __name__ == "__main__":
    main()